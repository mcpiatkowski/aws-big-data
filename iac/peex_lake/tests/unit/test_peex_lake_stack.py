import aws_cdk as core
import aws_cdk.assertions as assertions
from peex_lake.peex_lake_stack import PeexLakeStack


# example tests. To run these tests, uncomment this file along with the example
# resource in peex_lake/peex_lake_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = PeexLakeStack(app, "peex-lake")
    template = assertions.Template.from_stack(stack)


#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
