package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes (fileName, version, hashIndex, hashValue) VALUES (?, ?, ?, ?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {

	// Remove index.db file if it exists
	// log.Println(fileMetas)
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}

	// Create new index.db
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()

	// Start writing new meta to index.db
	insertStatement, err := db.Prepare(insertTuple)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	for _, v := range fileMetas {
		for i, h := range v.BlockHashList {
			insertStatement.Exec(v.Filename, v.Version, i, h)
		}
	}

	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `select DISTINCT filename, version from indexes`

const getTuplesByFileName string = `select hashValue
																		from indexes
																		where fileName = ?
																		order by hashIndex`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
// TODO optimization: maybe merge into 1 sql request instead of 2? (use "order fileName hashIndex")
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {

	// Build metaFile path
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))

	fileMetaMap = make(map[string]*FileMetaData)

	// Check if metaFile path is valid
	metaFileStats, e := os.Stat(metaFilePath)
	if os.IsNotExist(e) {
		_, e = os.Create(metaFilePath)
		if e != nil {
			return fileMetaMap, e
		}
		return fileMetaMap, nil
	} else if e != nil {
		return fileMetaMap, nil
	}

	if metaFileStats.IsDir() {
		return fileMetaMap, fmt.Errorf("%v is a directory", metaFilePath)
	}

	// Open db and start reading
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		// log.Fatal("Error When Opening Meta")
		return fileMetaMap, nil
	}

	// Get file(Name)s list
	rows, err := db.Query(getDistinctFileName)
	if err != nil {
		return fileMetaMap, nil
	}

	// Get hashList for each file
	var fileName string
	var version int32
	for rows.Next() {

		rows.Scan(&fileName, &version)
		hashes, err := db.Query(getTuplesByFileName, fileName)
		if err != nil {
			// log.Fatal("Error When Opening Meta")
			return fileMetaMap, nil
		}

		var hashesList = make([]string, 0)
		var tmpHash string
		for hashes.Next() {
			hashes.Scan(&tmpHash)
			hashesList = append(hashesList, tmpHash)
		}

		fileMetaMap[fileName] = &FileMetaData{Filename: fileName, Version: version}
		fileMetaMap[fileName].BlockHashList = hashesList

	}

	return fileMetaMap, nil

}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
