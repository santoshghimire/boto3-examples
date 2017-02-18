from botocore import session
import json


class StateFunctionWrapper(object):
    """A wrapper for State Function"""
    def __init__(self, *args, **kwargs):
        self._session = session.get_session()
        region = self.kwargs.get('region_name', 'us-east-1')
        self.client = self._session.create_client(
            'stepfunctions', region_name=region)

    def create_state_machine(self, name, definition, role_arn):
        """
        Create a state machine.
        PARAMS
        @name: name of the state machine
        @defination: json definition of the state machine
        @role_arn: Arn of the role created for this state machine
        """
        response = self.client.create_state_machine(
            name=name,
            definition=json.dumps(definition),
            roleArn=role_arn
        )
        sm_arn = response.get('stateMachineArn')
        if sm_arn:
            print('State Machine {0} with arn {1} created successfully'.format(
                name, sm_arn
            ))
        return sm_arn

    def get_state_machine(self, name):
        """
        Get a state machine given its name
        """
        response = self.client.list_state_machines()
        if not response.get('stateMachines'):
            return None
        for sm in response.get('stateMachines'):
            if sm['name'] == name:
                return sm['stateMachineArn']

    def create_execution(self, sm_arn, input_data):
        """
        Create an execution for a state machine.
        PARAMS
        @sm_arn: Arn of the state machine that is to be executed
        @input_data: Input json data to be passed for the execution
        """
        execution_response = self.client.start_execution(
            stateMachineArn=sm_arn,
            input=json.dumps(input_data)
        )
        execution_arn = execution_response.get('executionArn')
        return execution_arn

    def dummy_state_machine(self, sm_name, lambda_1_arn, lambda_2_arn):
        """
        Create a dummy state machine
        https://states-language.net/spec.html
        """
        state_function_definition = {
            "Comment": "A dummy state machine",
            "StartAt": "State1",
            "States": {
                "State1": {
                    "Resource": lambda_1_arn,
                    "Type": "Task",
                    "Next": "State2"
                },
                "State2": {
                    "Type": "Task",
                    "Resource": lambda_2_arn,
                    "Next": "End"
                },
                "End": {
                    "Type": "Succeed"
                }
            }
        }

        with open('/tmp/dummy-sm.json', 'wb') as jsonfile:
            json.dump(state_function_definition, jsonfile, indent=4)
        self.create_state_machine(
            name=sm_name, definition=state_function_definition
        )
