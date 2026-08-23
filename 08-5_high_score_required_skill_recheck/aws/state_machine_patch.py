#!/usr/bin/env python3
"""Apply the Issue 2 Batch-wait/Phase-B states to the existing production ASL."""

import argparse
import json
from pathlib import Path
from typing import Any, Dict

import boto3


DEFAULT_REGION = "ap-northeast-1"
DEFAULT_STATE_MACHINE_ARN = (
    "arn:aws:states:ap-northeast-1:166714029268:stateMachine:"
    "auto-match-llm-classifier-pipeline-orchestration"
)
DEFAULT_LAMBDA_ARN = (
    "arn:aws:lambda:ap-northeast-1:166714029268:function:"
    "auto-match-08-5-batch-status"
)
INSTANCE_ID = "i-06c075528d0039c3a"
LOG_GROUP = "/aws/ssm/auto-match-llm-classifier-pipeline"


class PatchError(RuntimeError):
    pass


def _retry() -> Any:
    return [
        {
            "ErrorEquals": ["States.TaskFailed", "States.Timeout"],
            "IntervalSeconds": 3,
            "MaxAttempts": 3,
            "BackoffRate": 2.0,
        }
    ]


def _ec2_task(action: str, result_path: str, next_state: str, failure: str) -> Dict[str, Any]:
    return {
        "Type": "Task",
        "Resource": f"arn:aws:states:::aws-sdk:ec2:{action}",
        "Parameters": {"InstanceIds": [INSTANCE_ID]},
        "ResultPath": result_path,
        "TimeoutSeconds": 120 if action != "describeInstances" else 30,
        "Retry": _retry(),
        "Catch": [
            {"ErrorEquals": ["States.ALL"], "ResultPath": "$.caught_error", "Next": failure}
        ],
        "Next": next_state,
    }


def _failure_state(code: str, message: str, next_state: str = "StopEC2AfterFailure") -> Dict[str, Any]:
    return {
        "Type": "Pass",
        "Result": {"code": code, "message": message},
        "ResultPath": "$.failure",
        "Next": next_state,
    }


def _phase_b_states(lambda_arn: str) -> Dict[str, Any]:
    return {
        "CheckBatchWaitCurrentStep": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.poll.status_document.current_step",
                    "StringEquals": "08-5_BATCH_WAIT",
                    "Next": "StopEC2ForBatchWait",
                }
            ],
            "Default": "CheckRunningWaitLimit",
        },
        "StopEC2ForBatchWait": _ec2_task(
            "stopInstances", "$.batch_wait_stop_result", "WaitForBatchStatus", "SetStopForBatchWaitFailure"
        ),
        "SetStopForBatchWaitFailure": _failure_state(
            "STOP_FOR_BATCH_WAIT_FAILED",
            "Phase A suspended, but EC2 StopInstances failed.",
            "PublishFailureNotification",
        ),
        "WaitForBatchStatus": {"Type": "Wait", "Seconds": 300, "Next": "InvokeBatchStatusLambda"},
        "InvokeBatchStatusLambda": {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke",
            "Parameters": {
                "FunctionName": lambda_arn,
                "Payload": {"run_id.$": "$.run_id", "run_date.$": "$.run_date"},
            },
            "ResultSelector": {"result.$": "$.Payload"},
            "ResultPath": "$.batch_status_lambda",
            "TimeoutSeconds": 90,
            "Retry": [
                {
                    "ErrorEquals": [
                        "Lambda.ServiceException",
                        "Lambda.AWSLambdaException",
                        "Lambda.SdkClientException",
                        "Lambda.TooManyRequestsException",
                        "States.Timeout",
                    ],
                    "IntervalSeconds": 5,
                    "MaxAttempts": 3,
                    "BackoffRate": 2.0,
                }
            ],
            "Catch": [
                {
                    "ErrorEquals": ["States.ALL"],
                    "ResultPath": "$.caught_error",
                    "Next": "SetBatchStatusLambdaFailure",
                }
            ],
            "Next": "CheckBatchStatusOutcome",
        },
        "CheckBatchStatusOutcome": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.batch_status_lambda.result.outcome",
                    "StringEquals": "WAIT",
                    "Next": "WaitForBatchStatus",
                },
                {
                    "Variable": "$.batch_status_lambda.result.outcome",
                    "StringEquals": "COMPLETED",
                    "Next": "PreparePhaseBStart",
                },
                {
                    "Variable": "$.batch_status_lambda.result.outcome",
                    "StringEquals": "FAILED",
                    "Next": "SetBatchTerminalFailure",
                },
            ],
            "Default": "SetUnknownBatchStatusOutcomeFailure",
        },
        "SetBatchStatusLambdaFailure": _failure_state(
            "BATCH_STATUS_LAMBDA_FAILED",
            "08-5 Batch status Lambda failed; EC2 remains stopped.",
            "PublishFailureNotification",
        ),
        "SetBatchTerminalFailure": {
            "Type": "Pass",
            "Parameters": {
                "code": "BATCH_TERMINAL_FAILURE",
                "message.$": "States.Format('08-5 Batch stopped safely: status={} reason={}', $.batch_status_lambda.result.batch_status, $.batch_status_lambda.result.reason)",
            },
            "ResultPath": "$.failure",
            "Next": "PublishFailureNotification",
        },
        "SetUnknownBatchStatusOutcomeFailure": _failure_state(
            "BATCH_STATUS_OUTCOME_UNKNOWN",
            "08-5 Batch status Lambda returned an unknown outcome; EC2 remains stopped.",
            "PublishFailureNotification",
        ),
        "PreparePhaseBStart": {
            "Type": "Pass",
            "Parameters": {
                "run_id.$": "$.run_id",
                "run_date.$": "$.run_date",
                "phase_b_waits": {"ec2_running": 0, "ssm_ready": 0, "launcher": 0},
            },
            "Next": "StartEC2ForPhaseB",
        },
        "StartEC2ForPhaseB": _ec2_task(
            "startInstances", "$.phase_b_start_result", "WaitForPhaseBEC2Running", "SetPhaseBStartFailure"
        ),
        "SetPhaseBStartFailure": _failure_state(
            "PHASE_B_START_FAILED",
            "Batch completed, but EC2 StartInstances for Phase B failed.",
            "PublishFailureNotification",
        ),
        "WaitForPhaseBEC2Running": {"Type": "Wait", "Seconds": 60, "Next": "DescribePhaseBEC2"},
        "DescribePhaseBEC2": _ec2_task(
            "describeInstances", "$.phase_b_describe_result", "IsPhaseBEC2Running", "SetPhaseBDescribeFailure"
        ),
        "SetPhaseBDescribeFailure": _failure_state(
            "PHASE_B_DESCRIBE_FAILED", "DescribeInstances failed during Phase B startup."
        ),
        "IsPhaseBEC2Running": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.phase_b_describe_result.Reservations[0].Instances[0].State.Name",
                    "StringEquals": "running",
                    "Next": "WaitForPhaseBSSMReady",
                }
            ],
            "Default": "IncrementPhaseBEC2Wait",
        },
        "IncrementPhaseBEC2Wait": {
            "Type": "Pass",
            "Parameters": {
                "run_id.$": "$.run_id",
                "run_date.$": "$.run_date",
                "phase_b_waits": {
                    "ec2_running.$": "States.MathAdd($.phase_b_waits.ec2_running, 1)",
                    "ssm_ready.$": "$.phase_b_waits.ssm_ready",
                    "launcher.$": "$.phase_b_waits.launcher",
                },
            },
            "Next": "CheckPhaseBEC2WaitLimit",
        },
        "CheckPhaseBEC2WaitLimit": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.phase_b_waits.ec2_running",
                    "NumericGreaterThanEquals": 15,
                    "Next": "SetPhaseBEC2TimeoutFailure",
                }
            ],
            "Default": "WaitForPhaseBEC2Running",
        },
        "SetPhaseBEC2TimeoutFailure": _failure_state(
            "PHASE_B_EC2_TIMEOUT", "EC2 did not reach running state for Phase B within 15 minutes."
        ),
        "WaitForPhaseBSSMReady": {"Type": "Wait", "Seconds": 30, "Next": "CheckPhaseBSSMManagedInstance"},
        "CheckPhaseBSSMManagedInstance": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:ssm:describeInstanceInformation",
            "Parameters": {"Filters": [{"Key": "InstanceIds", "Values": [INSTANCE_ID]}]},
            "ResultPath": "$.phase_b_ssm_result",
            "TimeoutSeconds": 30,
            "Retry": _retry(),
            "Catch": [
                {"ErrorEquals": ["States.ALL"], "ResultPath": "$.caught_error", "Next": "SetPhaseBSSMDescribeFailure"}
            ],
            "Next": "IsPhaseBSSMReady",
        },
        "SetPhaseBSSMDescribeFailure": _failure_state(
            "PHASE_B_SSM_DESCRIBE_FAILED", "DescribeInstanceInformation failed during Phase B startup."
        ),
        "IsPhaseBSSMReady": {
            "Type": "Choice",
            "Choices": [
                {
                    "And": [
                        {"Variable": "$.phase_b_ssm_result.InstanceInformationList[0].PingStatus", "IsPresent": True},
                        {"Variable": "$.phase_b_ssm_result.InstanceInformationList[0].PingStatus", "StringEquals": "Online"},
                    ],
                    "Next": "SendPhaseBLauncherCommand",
                }
            ],
            "Default": "IncrementPhaseBSSMWait",
        },
        "IncrementPhaseBSSMWait": {
            "Type": "Pass",
            "Parameters": {
                "run_id.$": "$.run_id",
                "run_date.$": "$.run_date",
                "phase_b_waits": {
                    "ec2_running.$": "$.phase_b_waits.ec2_running",
                    "ssm_ready.$": "States.MathAdd($.phase_b_waits.ssm_ready, 1)",
                    "launcher.$": "$.phase_b_waits.launcher",
                },
            },
            "Next": "CheckPhaseBSSMWaitLimit",
        },
        "CheckPhaseBSSMWaitLimit": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.phase_b_waits.ssm_ready",
                    "NumericGreaterThanEquals": 30,
                    "Next": "SetPhaseBSSMTimeoutFailure",
                }
            ],
            "Default": "WaitForPhaseBSSMReady",
        },
        "SetPhaseBSSMTimeoutFailure": _failure_state(
            "PHASE_B_SSM_TIMEOUT", "SSM did not become Online for Phase B within 15 minutes."
        ),
        "SendPhaseBLauncherCommand": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:ssm:sendCommand",
            "Parameters": {
                "InstanceIds": [INSTANCE_ID],
                "DocumentName": "AWS-RunShellScript",
                "Comment": "Launch SES matching pipeline Phase B managed systemd unit",
                "Parameters": {
                    "commands.$": "States.Array(States.Format('/usr/bin/env PIPELINE_PHASE=B RUN_ID={} RUN_DATE={} /usr/bin/bash /home/ec2-user/pipeline_ses_steps/00_pipeline/00_tool/launch_full_pipeline_async.sh', $.run_id, $.run_date))",
                    "executionTimeout": ["300"],
                },
                "CloudWatchOutputConfig": {
                    "CloudWatchLogGroupName": LOG_GROUP,
                    "CloudWatchOutputEnabled": True,
                },
            },
            "ResultPath": "$.phase_b_send_result",
            "TimeoutSeconds": 60,
            "Retry": _retry(),
            "Catch": [
                {"ErrorEquals": ["States.ALL"], "ResultPath": "$.caught_error", "Next": "SetPhaseBSendFailure"}
            ],
            "Next": "WaitForPhaseBLauncherCommand",
        },
        "SetPhaseBSendFailure": _failure_state(
            "PHASE_B_SEND_FAILED", "SSM SendCommand for Phase B failed."
        ),
        "WaitForPhaseBLauncherCommand": {"Type": "Wait", "Seconds": 10, "Next": "GetPhaseBLauncherCommandInvocation"},
        "GetPhaseBLauncherCommandInvocation": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:ssm:getCommandInvocation",
            "Parameters": {
                "CommandId.$": "$.phase_b_send_result.Command.CommandId",
                "InstanceId": INSTANCE_ID,
            },
            "ResultPath": "$.phase_b_command_result",
            "TimeoutSeconds": 30,
            "Retry": [
                {
                    "ErrorEquals": ["Ssm.InvocationDoesNotExist", "Ssm.InvocationDoesNotExistException"],
                    "IntervalSeconds": 2,
                    "MaxAttempts": 3,
                    "BackoffRate": 2.0,
                }
            ],
            "Catch": [
                {
                    "ErrorEquals": ["Ssm.InvocationDoesNotExist", "Ssm.InvocationDoesNotExistException"],
                    "ResultPath": "$.caught_error",
                    "Next": "IncrementPhaseBLauncherWait",
                },
                {"ErrorEquals": ["States.ALL"], "ResultPath": "$.caught_error", "Next": "SetPhaseBGetLauncherFailure"},
            ],
            "Next": "CheckPhaseBLauncherStatus",
        },
        "SetPhaseBGetLauncherFailure": _failure_state(
            "PHASE_B_GET_LAUNCHER_FAILED", "GetCommandInvocation for Phase B failed."
        ),
        "CheckPhaseBLauncherStatus": {
            "Type": "Choice",
            "Choices": [
                {"Variable": "$.phase_b_command_result.Status", "StringEquals": "Success", "Next": "InitializePhaseBStatusPolling"},
                {
                    "Or": [
                        {"Variable": "$.phase_b_command_result.Status", "StringEquals": value}
                        for value in ("Failed", "Cancelled", "TimedOut", "ExecutionTimedOut", "DeliveryTimedOut")
                    ],
                    "Next": "SetPhaseBLauncherStatusFailure",
                },
                {
                    "Or": [
                        {"Variable": "$.phase_b_command_result.Status", "StringEquals": value}
                        for value in ("Pending", "InProgress", "Delayed", "Cancelling")
                    ],
                    "Next": "IncrementPhaseBLauncherWait",
                },
            ],
            "Default": "SetPhaseBLauncherStatusFailure",
        },
        "SetPhaseBLauncherStatusFailure": _failure_state(
            "PHASE_B_LAUNCHER_FAILED", "Phase B launcher returned failure or an unknown status."
        ),
        "IncrementPhaseBLauncherWait": {
            "Type": "Pass",
            "Parameters": {
                "run_id.$": "$.run_id",
                "run_date.$": "$.run_date",
                "phase_b_send_result.$": "$.phase_b_send_result",
                "phase_b_waits": {
                    "ec2_running.$": "$.phase_b_waits.ec2_running",
                    "ssm_ready.$": "$.phase_b_waits.ssm_ready",
                    "launcher.$": "States.MathAdd($.phase_b_waits.launcher, 1)",
                },
            },
            "Next": "CheckPhaseBLauncherWaitLimit",
        },
        "CheckPhaseBLauncherWaitLimit": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.phase_b_waits.launcher",
                    "NumericGreaterThanEquals": 30,
                    "Next": "SetPhaseBLauncherTimeoutFailure",
                }
            ],
            "Default": "WaitForPhaseBLauncherCommand",
        },
        "SetPhaseBLauncherTimeoutFailure": _failure_state(
            "PHASE_B_LAUNCHER_TIMEOUT", "Phase B launcher did not complete within 5 minutes."
        ),
        "InitializePhaseBStatusPolling": {
            "Type": "Pass",
            "Parameters": {
                "run_id.$": "$.run_id",
                "run_date.$": "$.run_date",
                "launcher_command_id.$": "$.phase_b_send_result.Command.CommandId",
                "poll": {
                    "status_key.$": "States.Format('pipeline_ses_steps/pipeline-status/{}/{}/status.json', $.run_date, $.run_id)",
                    "missing_waits": 0,
                    "running_waits": 0,
                },
            },
            "Next": "WaitForPhaseBStatusUpdate",
        },
        "WaitForPhaseBStatusUpdate": {"Type": "Wait", "Seconds": 30, "Next": "ListPipelineStatusObject"},
    }


def apply_patch(definition: Dict[str, Any], lambda_arn: str = DEFAULT_LAMBDA_ARN) -> Dict[str, Any]:
    if definition.get("StartAt") != "ValidateRunDateInput":
        raise PatchError("production State Machine StartAt anchor不一致")
    states = definition.get("States")
    if not isinstance(states, dict):
        raise PatchError("ASL States不正")
    required = {
        "ValidateRunningDocument",
        "CheckRunningWaitLimit",
        "StopEC2AfterFailure",
        "PublishFailureNotification",
        "ListPipelineStatusObject",
    }
    missing = sorted(required - set(states))
    if missing:
        raise PatchError(f"ASL anchor state欠落: {missing}")
    choices = states["ValidateRunningDocument"].get("Choices")
    if not isinstance(choices, list) or len(choices) != 1:
        raise PatchError("ValidateRunningDocument contract不一致")
    current_next = choices[0].get("Next")
    if current_next not in ("CheckRunningWaitLimit", "CheckBatchWaitCurrentStep"):
        raise PatchError("ValidateRunningDocument Next anchor不一致")
    choices[0]["Next"] = "CheckBatchWaitCurrentStep"
    states.update(_phase_b_states(lambda_arn))
    # The OpenAI completion window is 24h; retain time to observe expiry and run Phase B.
    definition["TimeoutSeconds"] = max(int(definition.get("TimeoutSeconds", 0)), 108000)
    return definition


def validate_graph(definition: Dict[str, Any]) -> None:
    states = definition.get("States")
    if not isinstance(states, dict):
        raise PatchError("ASL States不正")
    targets = []
    for state in states.values():
        for key in ("Next", "Default"):
            if key in state:
                targets.append(state[key])
        for branch in state.get("Choices", []):
            if "Next" in branch:
                targets.append(branch["Next"])
        for catcher in state.get("Catch", []):
            if "Next" in catcher:
                targets.append(catcher["Next"])
    missing = sorted({target for target in targets if target not in states})
    if missing:
        raise PatchError(f"ASL transition target欠落: {missing}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", type=Path)
    source.add_argument("--state-machine-arn", default=None)
    parser.add_argument("--lambda-arn", default=DEFAULT_LAMBDA_ARN)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.input:
        definition = json.loads(args.input.read_text(encoding="utf-8"))
    else:
        sfn = boto3.client("stepfunctions", region_name=args.region)
        response = sfn.describe_state_machine(
            stateMachineArn=args.state_machine_arn or DEFAULT_STATE_MACHINE_ARN
        )
        definition = json.loads(response["definition"])
    patched = apply_patch(definition, args.lambda_arn)
    validate_graph(patched)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(patched, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(str(args.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
