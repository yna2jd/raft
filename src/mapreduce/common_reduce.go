package mapreduce

import (
	"encoding/json"
	"maps"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	{
		// You will need to write this function.
		// You can find the intermediate file for this reduce task from map task number
		// m using reduceName(jobName, m, reduceTaskNumber).
		// Remember that you've encoded the values in the intermediate files, so you
		// will need to decode them. If you chose to use JSON, you can read out
		// multiple decoded values by creating a decoder, and then repeatedly calling
		// .Decode() on it until Decode() returns an error.
		//
		// You should write the reduced output in as JSON encoded KeyValue
		// objects to a file named mergeName(jobName, reduceTaskNumber). We require
		// you to use JSON here because that is what the merger than combines the
		// output from all the reduce tasks expects. There is nothing "special" about
		// JSON -- it is just the marshalling format we chose to use. It will look
		// something like this:
		//
		// enc := json.NewEncoder(mergeFile)
		// for key in ... {
		// 	enc.Encode(KeyValue{key, reduceF(...)})
		// }
		// file.Close()
		//
		// Use checkError to handle errors.
	}

	// read from map files
	keyValMap := map[string][]string{}
	for m := 0; m < nMap; m++ {
		file, err := os.Open(reduceName(jobName, m, reduceTaskNumber))
		checkError(err)
		enc := json.NewDecoder(file)
		keyValue := KeyValue{}
		for err := enc.Decode(&keyValue); err == nil; err = enc.Decode(&keyValue) {
			keyValMap[keyValue.Key] = append(keyValMap[keyValue.Key], keyValue.Value)
			checkError(err)
		}
		checkError(err)
		err = file.Close()
		checkError(err)
	}

	// write to merge file
	mergeFile, err := os.Create(mergeName(jobName, reduceTaskNumber))
	checkError(err)
	enc := json.NewEncoder(mergeFile)
	for key := range maps.Keys(keyValMap) {
		err := enc.Encode(KeyValue{key, reduceF(key, keyValMap[key])})
		checkError(err)
	}
	err = mergeFile.Close()
	checkError(err)

}
