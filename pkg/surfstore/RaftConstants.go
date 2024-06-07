package surfstore

import (
	"fmt"
)

var ERR_SERVER_CRASHED = fmt.Errorf("Server is crashed.")
var ERR_NOT_LEADER = fmt.Errorf("Server is not the leader")

var ERR_NOT_MAJORITY = fmt.Errorf("Failed on majority of nodes are working/succeed")
