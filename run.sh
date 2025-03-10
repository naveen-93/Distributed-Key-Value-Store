# First server
go run pkg/server/main.go -addr :50051 -id node1 >> /dev/null 2>&1 &

# Second server (knows about first)
go run pkg/server/main.go -addr :50052 -id node2 -peers "node1@localhost:50051"  >> /dev/null 2>&1 &

# Third server (knows about first and second)
go run pkg/server/main.go -addr :50053 -id node3 -peers "node1@localhost:50051,node2@localhost:50052,node3@localhost:50053" >> /dev/null 2>&1 &