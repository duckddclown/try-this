package surfstore

import (
	context "context"
	"fmt"
	"log"
	"sync"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64              // Curent term
	log           []*UpdateOperation // Log of operations

	metaStore *MetaStore

	// New fields

	// Server (this and peers) identification info

	id    int64    // Server id
	peers []string // Server list (including me)

	// Volatile states for all servers

	commitIndex int64 // index of latest committed log entry
	lastApplied int64 // index of latest applied   log entry

	// Volatile state for leader
	nextIndex    []int64
	MatchedIndex []int64

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) CheckCrashedOrNotLeaderState(ctx context.Context) (*Success, error) {
	s.isCrashedMutex.RLock()
	s.isLeaderMutex.RLock()
	defer s.isCrashedMutex.RUnlock()
	defer s.isLeaderMutex.RUnlock()
	if s.isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	if !s.isLeader {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) CheckCrashed(ctx context.Context) (*Success, error) {
	s.isCrashedMutex.RLock()
	defer s.isCrashedMutex.RUnlock()
	if s.isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	return &Success{Flag: true}, nil
}

// Returns metadata from the filesystem
// If leader,
//
//	and if a majority of the nodes are working:
//	-> return the correct answer
//	or if a majority of the nodes are crashed:
//	-> block until a majority recover
//
// If not the leader:
// -> should indicate an error back to the client
func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {

	// Checks if server is crashed and if server is not the leader
	succ, err := s.CheckCrashedOrNotLeaderState(ctx)
	if !succ.Flag {
		return nil, err
	}

	// Check if majority of the nodes are working
	// Block until condition is true
	for {
		succ, _ = s.SendHeartbeat(ctx, &emptypb.Empty{})
		if succ.Flag {
			break
		}
		fmt.Println("GetFileInfoMap - blocking requests - less than majority of nodes working")
	}

	// Get FileInfoMap from MetaStore
	result, err := s.metaStore.GetFileInfoMap(ctx, &emptypb.Empty{})
	if err != nil {
		fmt.Printf("GetFileInfoMap - Failed getting info from MetaStore - %v\n", err)
		return nil, err
	}

	return result, err
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	// Checks if server is crashed and if server is not the leader
	succ, err := s.CheckCrashedOrNotLeaderState(ctx)
	if !succ.Flag {
		return nil, err
	}

	// Check if majority of the nodes are working
	// Block until condition is true
	for {
		succ, _ = s.SendHeartbeat(ctx, &emptypb.Empty{})
		if succ.Flag {
			break
		}
		fmt.Println("GetBlockStoreMap - blocking requests - less than majority of nodes working")
	}

	// Get FileInfoMap from MetaStore
	result, err := s.metaStore.GetBlockStoreMap(ctx, hashes)
	if err != nil {
		fmt.Printf("GetBlockStoreMap - Failed getting info from MetaStore - %v\n", err)
		return nil, err
	}

	return result, err
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	// Checks if server is crashed and if server is not the leader
	succ, err := s.CheckCrashedOrNotLeaderState(ctx)
	if !succ.Flag {
		return nil, err
	}

	// Check if majority of the nodes are working
	// Block until condition is true
	for {
		succ, _ = s.SendHeartbeat(ctx, &emptypb.Empty{})
		if succ.Flag {
			break
		}
		fmt.Println("GetBlockStoreAddrs - blocking requests - less than majority of nodes working")
	}

	// Get FileInfoMap from MetaStore
	result, err := s.metaStore.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
	if err != nil {
		fmt.Printf("GetBlockStoreAddrs - Failed getting info from MetaStore - %v\n", err)
		return nil, err
	}

	return result, err
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {

	log.Println("--------------------------------------------------------")
	log.Printf("[%v] UpdateFile - DEBUG - Printing server's info:\n", s.id)
	log.Println(s)
	log.Println("--------------------------------------------------------")

	// Checks if server is crashed and if server is not the leader
	succ, err := s.CheckCrashedOrNotLeaderState(ctx)
	if !succ.Flag {
		return nil, err
	}

	// Append entry to log
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})
	committedChan := make(chan bool)
	// s.pendingCommits = append(s.pendingCommits, &committedChan)

	// Send entry to all followers in parallel
	go s.SendToAllFollowersInParallel(ctx, committedChan)

	// Keep trying indefinitely (even after responding)  ** rely on SendHeartbeat

	// Commit the entry once majority of followers have it in their log
	committed := <-committedChan

	// Once commited, apply to the state machine
	if committed {
		ver, err := s.metaStore.UpdateFile(ctx, filemeta)
		s.lastApplied = s.commitIndex
		return ver, err
	}

	return nil, nil
}

func (s *RaftSurfstore) SendToAllFollowersInParallel(ctx context.Context, committedChan chan bool) {
	// Send entry to all followers and count the replies

	responses := make(chan bool, len(s.peers)-1)
	// Contact all the follower, send some AppendEntries call
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}

		go s.SendToFollower(ctx, addr, responses)
	}

	totalResponses := 1
	totalAppendSuccesses := 1

	// log.Printf("Here!!!\nfilemeta: %v\nOther values: %v %v\n", filemeta, totalAppendSuccesses, len(s.peers))

	// Wait for all responses
	for {
		// for float64(totalAppendSuccesses) < (float64(len(s.peers)) / 2.0) {
		log.Printf("SendToAllFollowersInParallel - Receiving request...\n")
		result := <-responses
		totalResponses++
		if result {
			totalAppendSuccesses++
		}
		if totalResponses == len(s.peers) {
			break
		}
	}

	if totalAppendSuccesses > (len(s.peers) / 2) {
		// TODO: put on correct channel
		// *s.pendingCommits[0] <- true
		committedChan <- true
		// TODO: update commit Index correctly
		s.commitIndex++
	}
}

func (s *RaftSurfstore) SendToFollower(ctx context.Context, addr string, responses chan bool) {

	log.Printf("SendToFollower - Sending package to follower %v\n", addr)

	var prevLogTerm int64
	if s.commitIndex == -1 {
		prevLogTerm = 0
	} else {
		prevLogTerm = s.log[s.commitIndex].Term
	}

	dummyEntryInput := AppendEntryInput{
		Term: s.term,
		// TODO: Put in the right values
		PrevLogIndex: s.commitIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}

	// s.AppendHelper(ctx, &dummyEntryInput, addr, outputChan)

	for {
		log.Printf("SendToFollower - Start attempting connection to %v\n", addr)
		// TODO: check all errors
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			fmt.Printf("SendToFollower - Failed connecting to server %v, retrying...\n", err)
			continue
		}
		log.Printf("SendToFollower - Connection to peer %v succeed\n", addr)
		client := NewRaftSurfstoreClient(conn)
		log.Printf("SendToFollower - Try append entries for %v\n", addr)
		// log.Println(s)
		// log.Println(dummyEntryInput)

		output, err := client.AppendEntries(ctx, &dummyEntryInput)
		if err != nil {
			s.MatchedIndex[output.ServerId] = output.MatchedIndex
			log.Printf("SendToFollower - Failed when appending entries: %v\n", err)
			responses <- false
			continue
		} else {
			s.MatchedIndex[output.ServerId] = output.MatchedIndex
			responses <- true
			log.Printf("SendToFollower - Append entries for %v succeed, sent response\n", addr)
		}
		log.Printf("SendToFollower - Append entries for %v succeed, conclude\n", addr)
		break
	}
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {

	// Checks if server is crashed
	succ, err := s.CheckCrashed(ctx)
	if !succ.Flag {
		return nil, err
	}

	if s.term < input.Term {
		s.isLeaderMutex.Lock()
		defer s.isLeaderMutex.Unlock()
		s.isLeader = false
		s.term = input.Term
	}

	output := &AppendEntryOutput{
		ServerId:     s.id,
		Term:         s.term,
		Success:      false,
		MatchedIndex: 0,
	}

	// TODO: actually check entries
	// 1. Reply false if term < currentTerm (§5.1)
	if input.Term < s.term {
		return output, fmt.Errorf("input term is too old, received term \"%v\", but server has term \"%v\"", input.Term, s.term)
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if input.PrevLogIndex != -1 && s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
		return output, fmt.Errorf("cannot find entry at prevLogIndex (\"%v\") whose term matches prevLogTerm", input.PrevLogIndex)
	}

	output.Success = true

	// log.Println("----------------------------")
	// log.Println("s.log:", s.log)
	// log.Println("input:", input)

	// 3. If an existing entry conflicts with a new one (same index but different terms),
	//    delete the existing entry and all that follow it (§5.3)
	lastMatchIndex := int64(-1)
	for i, entry := range s.log {

		// Update the "matched" index so far
		s.lastApplied = int64(i - 1)

		// When input.Entries reaches its length, for example:
		//     i =  0 1 2 3 4 5 6
		// s.log = [1 1 2 3 4 6 7]
		// input = [1 1 2 3]
		// At i = 4 we will enter this case
		if len(input.Entries) < i+1 {
			s.log = s.log[:i]
			lastMatchIndex = int64(i - 1)
			break
		}

		// Found latest log entry where two logs disagree
		// => Delete existing entry and all that follow it
		// For example:
		//     i =  0 1 2 3 4 5 6
		// s.log = [1 1 2 3 4 6 7]
		// input = [1 1 2 3 5]
		// At i = 4 we will enter this case
		// s.log => [1 1 2 3], and start appending logs from that
		if entry != input.Entries[i] {
			s.log = s.log[:i]
			lastMatchIndex = int64(i - 1)
			break
		}

		// Finish comparing s.log and input.Entries
		// For example:
		//     i =  0 1 2 3 4 5 6
		// s.log = [1 1 2 3]
		// input = [1 1 2 3 4 6 7]
		// At i = 3 we will enter this case
		// i.e. we are at the end of s.log
		if len(s.log) == i+1 {
			lastMatchIndex = int64(i)
		}
	}

	// log.Println(lastMatchIndex)
	// log.Println("----------------------------")

	// 4. Append any new entries not already in the log
	if lastMatchIndex != int64(len(input.Entries)-1) {
		s.log = append(s.log, input.Entries[lastMatchIndex+1:]...)
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// i.e. Update the committed index after updating our log
	// In human words: we want to know if we have received up to the same logs
	// as the leader, by comparing leaderCommit with our own commitIndex, we
	// know our progress, and update our commitIndex to update our progress
	if input.LeaderCommit > s.commitIndex {
		if input.LeaderCommit < int64(len(s.log))-1 {
			// We are up-to-date with the committed entries by leader
			// And we have some extra entries in our s.log that are not yet committed (by majority)
			// Before: commitIndex < input.LeaderCommit < len(s.log) - 1
			// After:  commitIndex = input.LeaderCommit < len(s.log) - 1
			s.commitIndex = input.LeaderCommit
		} else {
			// The entries we got is still behind leader's commitIndex
			// i.e. there are some entries that are committed (by majority)
			//      but we still don't have them in our s.log
			// Before: commitIndex <= len(s.log) - 1 < input.LeaderCommit
			// After:  commitIndex =  len(s.log) - 1 < input.LeaderCommit
			s.commitIndex = int64(len(s.log)) - 1
		}
	}

	log.Println("--------------------------------------------------------")
	log.Printf("[%v] AppendEntries - DEBUG - Printing server's info:\n", s.id)
	log.Println(s)
	log.Println(input)
	log.Println("--------------------------------------------------------")
	// Update FileMetaData
	for s.lastApplied < s.commitIndex {
		entry := s.log[s.lastApplied+1]
		_, err := s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		if err != nil {
			fmt.Printf("AppendEntries - Error raised with udpating file: %v\n", err)
			return output, nil
		}
		s.lastApplied++
		output.MatchedIndex = s.lastApplied
	}

	return output, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {

	s.isCrashedMutex.RLock()
	s.isLeaderMutex.Lock()

	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		s.isLeaderMutex.Unlock()
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	s.isLeader = true
	s.term++
	s.isCrashedMutex.RUnlock()
	s.isLeaderMutex.Unlock()

	// return &Success{Flag: true}, nil

	return s.SendHeartbeat(ctx, &emptypb.Empty{})

}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {

	succ, err := s.CheckCrashedOrNotLeaderState(ctx)
	if !succ.Flag {
		return nil, err
	}

	fmt.Printf("Sending heartbeat from %v\n", s.id)

	// Create dummy input
	var dummyEntryInput AppendEntryInput
	if len(s.log) != 0 {
		dummyEntryInput = AppendEntryInput{
			Term:         s.term,
			PrevLogIndex: int64(len(s.log) - 1),
			PrevLogTerm:  s.log[len(s.log)-1].Term,
			Entries:      s.log,
			LeaderCommit: s.commitIndex,
		}
	} else {
		dummyEntryInput = AppendEntryInput{
			Term:         s.term,
			PrevLogIndex: -1,
			PrevLogTerm:  0,
			Entries:      s.log,
			LeaderCommit: s.commitIndex,
		}
	}

	// Contact all the follower, send some AppendEntries call
	outputChan := make(chan *AppendEntryOutput, len(s.peers)-1)
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		go s.AppendHelper(ctx, &dummyEntryInput, addr, outputChan)
	}

	// Count the number of responses & success
	totalReponses := 1
	totalSuccess := 1

	for {
		result := <-outputChan
		totalReponses++
		if result != nil && result.Success {
			totalSuccess++
			// Check if s is still the leader or got replaced
			if result.ServerId != -1 && result.Term > s.term {
				s.isLeaderMutex.RLock()
				s.isLeader = false
				defer s.isLeaderMutex.RUnlock()
				fmt.Printf("Leader state is replaced by server %v\n", result.ServerId)
				return &Success{Flag: false}, ERR_NOT_LEADER
			}
		}
		if totalReponses == len(s.peers) {
			break
		}
	}

	fmt.Printf("SendHeartbeat - %v/%v responses succeed\n", totalSuccess, totalReponses)
	if totalSuccess > (len(s.peers) / 2.0) {
		return &Success{Flag: true}, nil
	}

	return &Success{Flag: false}, ERR_NOT_MAJORITY
}

func (s *RaftSurfstore) AppendHelper(ctx context.Context, input *AppendEntryInput, addr string, outputChan chan *AppendEntryOutput) {

	tmplateOutput := AppendEntryOutput{
		ServerId:     s.id,
		Term:         s.term,
		Success:      false,
		MatchedIndex: -1,
	}

	// Dial grpc call
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("AppendHelper - failed when dialing grpc call: %v\n", err)
		outputChan <- &tmplateOutput
		return
	}
	defer conn.Close()

	// Create client
	client := NewRaftSurfstoreClient(conn)

	// Try append entries
	log.Println(input)
	output, err := client.AppendEntries(ctx, input)
	log.Println(output)
	if err != nil {
		fmt.Printf("AppendHelper - failed when appending entry: %v\n", err)
		outputChan <- &tmplateOutput
	}
	outputChan <- output
	// conn.Close()
	// return
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	log.Printf("**************** Server %v is crashed ****************\n", s.id)
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
