[![codecov](https://codecov.io/github/MohamedAbdeen21/steeleye-de-task/branch/main/graph/badge.svg?token=130LF1JAUR)](https://codecov.io/github/MohamedAbdeen21/steeleye-de-task)

# steeleye-de-task
A Data Engineering task for SteelEye internship

A very interesting task to say the least, enjoyed doing it as I learned more about AWS configurations, permissions, more ways to parse XML, and much more. Thanks to SteelEye for this opportunity.

## Code and assumptions
For the sake of KISS (Keep It Short and Simple), the code assumes some things to be always true and therefore doesn't check for them.

Assumption:
- The ZIP file always contains at least one XML file inside.
- The base URL and the target XML file are always valid XML.
- The XML tags always exist in both files.
- We need a local copy of the .csv before writing to S3.

The code checks the base URL in case of typos, checks if provided path to the CSV is valid and create it if its not, and checks if provided index is out of range.

Both the S3 Bucket and the Lambda Function are created beforehand, make sure that the Lambda Function has the correct permission to write logs and write to S3. Will try to attach a URL while submitting to the S3 bucket with the .csv file.