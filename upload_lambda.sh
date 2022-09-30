pip install --platform manylinux2014_x86_64 --target=my-lambda-function --implementation cp --python 3.9 --only-binary=:all: --upgrade -r requirements.txt 
zip -r ./my-deployment-package.zip my-lambda-function
aws lambda update-function-code --function-name steeleye-de-task --zip-file fileb://my-deployment-package.zip
