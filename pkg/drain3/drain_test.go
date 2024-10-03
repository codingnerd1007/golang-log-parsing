package drain3

import (
	"bufio"
	"context"
	"fmt"
	"github.com/jaeyo/go-drain3/pkg/masker"
	"github.com/stretchr/testify/require"
	"log"
	"os"
	"testing"
	"time"
)

func TestDrain(t *testing.T) {
	drain, err := NewDrain(WithSimTh(0.5))
	require.NoError(t, err)

	miner := NewTemplateMiner(drain, NewMemoryPersistence())

	//logs := []string{
	//	"[ProducerStateManager partition=__consumer_offsets-48] Writing producer snapshot at offset 4339939698 (kafka.log.ProducerStateManager)",
	//	"[Log partition=__consumer_offsets-48, dir=/home1/irteam/apps/data/kafka/kafka-logs] Rolled new log segment at offset 4339939698 in 3 ms. (kafka.log.Log)",
	//	"[Log partition=__consumer_offsets-48, dir=/home1/irteam/apps/data/kafka/kafka-logs] Deleting segment files LogSegment(baseOffset=0, size=0, lastModifiedTime=1645674584000, largestRecordTimestamp=None) (kafka.log.Log)",
	//	"Deleted log /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000000000000000.log.deleted. (kafka.log.LogSegment)",
	//	"Deleted offset index /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000000000000000.index.deleted. (kafka.log.LogSegment)",
	//	"Deleted time index /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000000000000000.timeindex.deleted. (kafka.log.LogSegment)",
	//	"[Log partition=__consumer_offsets-48, dir=/home1/irteam/apps/data/kafka/kafka-logs] Deleting segment files LogSegment(baseOffset=2147429227, size=0, lastModifiedTime=1710735195000, largestRecordTimestamp=None) (kafka.log.Log)",
	//	"Deleted log /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000002147429227.log.deleted. (kafka.log.LogSegment)",
	//	"Deleted offset index /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000002147429227.index.deleted. (kafka.log.LogSegment)",
	//	"Deleted time index /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000002147429227.timeindex.deleted. (kafka.log.LogSegment)",
	//	"[ProducerStateManager partition=__consumer_offsets-49] Writing producer snapshot at offset 4339698 (kafka.log.ProducerStateManager)",
	//	"[Log partition=__consumer_offsets-48, dir=/home1/irteam/apps/data/kafka/kafka-logs] Deleting segment files LogSegment(baseOffset=4294790577, size=2703, lastModifiedTime=1711832815000, largestRecordTimestamp=Some(1710827112244)) (kafka.log.Log)",
	//	"[Log partition=__consumer_offsets-48, dir=/home1/irteam/apps/data/kafka/kafka-logs] Deleting segment files LogSegment(baseOffset=4338631022, size=641, lastModifiedTime=1711849197000, largestRecordTimestamp=Some(1711849197921)) (kafka.log.Log)",
	//	"Deleted log /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000004294790577.log.deleted. (kafka.log.LogSegment)",
	//	"Deleted log /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000004338631022.log.deleted. (kafka.log.LogSegment)",
	//	"Deleted offset index /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000004294790577.index.deleted. (kafka.log.LogSegment)",
	//	"Deleted offset index /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000004338631022.index.deleted. (kafka.log.LogSegment)",
	//	"Deleted time index /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000004294790577.timeindex.deleted. (kafka.log.LogSegment)",
	//	"Deleted time index /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000004338631022.timeindex.deleted. (kafka.log.LogSegment)",
	//	"[Log partition=__consumer_offsets-48, dir=/home1/irteam/apps/data/kafka/kafka-logs] Deleting segment files LogSegment(baseOffset=4339285360, size=104857589, lastModifiedTime=1711865580000, largestRecordTimestamp=Some(1711865580112)) (kafka.log.Log)",
	//	"Deleted log /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000004339285360.log.deleted. (kafka.log.LogSegment)",
	//	"Deleted offset index /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000004339285360.index.deleted. (kafka.log.LogSegment)",
	//	"Deleted time index /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000004339285360.timeindex.deleted. (kafka.log.LogSegment)",
	//	"[Log partition=__consumer_offsets-49, dir=/home1/irteam/apps/data/kafka/kafka-logs] Rolled new log segment at offset 432939698 in 2 ms. (kafka.log.Log)",
	//}

	file, err := os.Open("/home/rahil/TASKS/R&D/log-parsing/git-projects/go-drain3/resources/forti.log") // Replace with the path to your log file
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// Define the rules for matching and masking as a map
	rules := map[string]string{
		"((?<=[^A-Za-z0-9])|^)(([0-9a-f]{2,}:){3,}([0-9a-f]{2,}))((?=[^A-Za-z0-9])|$)":                                                       "<ID>",
		"((?<=[^A-Za-z0-9])|^)(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})((?=[^A-Za-z0-9])|$)":                                               "<IP>",
		"((?<=[^A-Za-z0-9])|^)([0-9a-f]{6,} ?){3,}((?=[^A-Za-z0-9])|$)":                                                                      "<SEQ>",
		"((?<=[^A-Za-z0-9])|^)([0-9A-F]{4} ?){4,}((?=[^A-Za-z0-9])|$)":                                                                       "<SEQ>",
		"((?<=[^A-Za-z0-9])|^)(0x[a-f0-9A-F]+)((?=[^A-Za-z0-9])|$)":                                                                          "<HEX>",
		"((?<=[^A-Za-z0-9])|^)([\\-\\+]?\\d+)((?=[^A-Za-z0-9])|$)":                                                                           "<NUM>",
		"(?<=executed cmd )(\".+?\")":                                                                                                        "<CMD>",
		"((?<=[^A-Za-z0-9])|^)(((https|http):\\/\\/|www[.])[A-Za-z0-9+&@#\\/%?=~_()-|!:,.;]*[-A-Za-z0-9+&@#\\/%=~_()|])((?=[^A-Za-z0-9])|$)": "<URL>",
		"((?<=[^A-Za-z0-9])|^)([\\w]{8}\\b-[\\w]{4}\\b-[\\w]{4}\\b-[\\w]{4}\\b-[\\w]{12})((?=[^A-Za-z0-9])|$)":                               "<GUID>",
		"((?<=[^A-Za-z0-9])|^)([\\w+\\.+\\-]+@+[\\w+\\.+\\-]+[\\.\\w]{2,})((?=[^A-Za-z0-9])|$)":                                              "<CMD>",
	}

	// Compile regex patterns once using the masker logic
	compiledRules, err := masker.CompileMaskRules(rules)
	if err != nil {
		log.Fatalf("Error compiling rules: %v", err)
	}

	// Read the file line by line
	scanner := bufio.NewScanner(file)
	ctx := context.Background()

	elaspedMaskingTime := int64(0)

	processingStartTime := time.Now()

	for scanner.Scan() {
		logMessage := scanner.Text()

		// Apply masking rules to the current log line
		maskingStartTime := time.Now()

		maskedLog := masker.ApplyMask(logMessage, compiledRules)

		elaspedMaskingTime += time.Since(maskingStartTime).Milliseconds()

		_, _, template, _, err := miner.AddLogMessage(ctx, maskedLog)
		require.NoError(t, err)

		params := miner.ExtractParameters(template, maskedLog)

		fmt.Println(params)

		//require.NotNil(t, params)

		// Print the masked log (or you can write to another file)
		fmt.Println("maskedLog : " + maskedLog)
	}

	proccessingTime := time.Since(processingStartTime).Milliseconds()

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading file: %v", err)
	}

	clusters := miner.drain.GetClusters()
	fmt.Println("total clusters: ", len(clusters))
	fmt.Println("total time taken for masking: ", elaspedMaskingTime)
	fmt.Println("total time taken for processing: ", proccessingTime)
	//require.Equal(t, 5, len(clusters))
}
