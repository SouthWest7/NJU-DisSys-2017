#!/bin/bash

# Check if Go is installed
if ! command -v go &> /dev/null; then
    echo "Go is not installed. Please install Go and try again."
    exit 1
fi

# Run the specific test with the provided filter
echo "Running tests with filter: Election"
# Part I
go test -run Election
# Part II
go test -run BasicAgree
go test -run FailAgree
go test -run ConcurrentStarts
go test -run Rejoin
go test -run Backup
go test -run Count
# Part III
go test -run Persist1
go test -run Persist2
go test -run Persist3

# Capture the exit code of the test command
exit_code=$?

# Output the test result
if [ $exit_code -eq 0 ]; then
    echo "Tests passed successfully."
else
    echo "Some tests failed. Please check the output above."
fi

# Exit with the same status as the test command
exit $exit_code
