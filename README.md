## Initialize version of the package
git tag v1.0.0
git push origin v1.0.0

## Update version of the package
git tag -a v1.0.1 -m "Refactor package structure"
git push origin v1.0.1

## To get the package from a project
go get github.com/engpap/kafka-wrapper-go@v1.0.1