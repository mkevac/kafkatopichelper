package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "kafkatopichelper",
	Short: "kafkatopichelper",
	Long:  `kafkatopichelper`,
	Run: func(cmd *cobra.Command, args []string) {
		// Do Stuff Here
	},
}

var getCmd = &cobra.Command{
	Use:   "get",
	Short: "get config for topic",
	Long:  `get config for topic`,
	Run: func(cmd *cobra.Command, args []string) {
		getCmdHandler(cmd, args)
	},
}

var setCmd = &cobra.Command{
	Use:   "set",
	Short: "set config for topic",
	Long:  `set config for topic`,
	Run: func(cmd *cobra.Command, args []string) {
		setCmdHandler(cmd, args)
	},
}

var topicsCmd = &cobra.Command{
	Use:   "topics",
	Short: "get topic list",
	Long:  `get topic list`,
	Run: func(cmd *cobra.Command, args []string) {
		topicsCmdHandler(cmd, args)
	},
}

var config struct {
	brokers    string
	topic      string
	names      string
	values     string
	debug      bool
	dryRun     bool
	topicsFile string
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&config.brokers, "brokers", "b", "", "brokers (i.e. \"kafka1.mlan:9092,kafka2.mlan:9092\")")
	rootCmd.PersistentFlags().BoolVarP(&config.debug, "debug", "d", false, "print more info")

	getCmd.Flags().StringVarP(&config.names, "names", "n", "", "config names (i.e. \"compression.type,min.insync.replicas\")")
	getCmd.Flags().StringVarP(&config.topic, "topic", "t", "", "topic (i.e. \"meetmaker-bg\")")
	getCmd.Flags().StringVarP(&config.topicsFile, "topicsFile", "f", "", "file with topic names (comma separated or 1 topic per line)")

	setCmd.Flags().StringVarP(&config.values, "values", "v", "", "config names and vales (i.e. \"retention.ms=604800001,min.insync.replicas=3\")")
	setCmd.Flags().StringVarP(&config.topic, "topic", "t", "", "topic (i.e. \"meetmaker-bg\")")
	setCmd.Flags().StringVarP(&config.topicsFile, "topicsFile", "f", "", "file with topic names to be updated (comma separated or 1 topic per line)")
	setCmd.Flags().BoolVarP(&config.dryRun, "dryRun", "r", false, "show config to be commited with setted values, don't update topic values in kafka")

	rootCmd.AddCommand(getCmd)
	rootCmd.AddCommand(setCmd)
	rootCmd.AddCommand(topicsCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func topicsFromFile(fileName string) ([]string, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to open file '%s': %s\n", fileName, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	var topics []string
	for scanner.Scan() {
		topics = append(topics, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to parse file '%s': %s\n", fileName, err)
	}

	return topics, nil
}

func topicsFromConfig() ([]string, error) {
	if config.topicsFile != "" {
		return topicsFromFile(config.topicsFile)
	}

	if config.topic == "" {
		return nil, fmt.Errorf("topics are not set\n")
	}

	return []string{config.topic}, nil
}

func newClusterAdmin() sarama.ClusterAdmin {
	addrs := strings.Split(config.brokers, ",")

	c := sarama.NewConfig()
	c.Version = sarama.V2_1_0_0

	admin, err := sarama.NewClusterAdmin(addrs, c)
	if err != nil {
		log.Fatalf("error while creating new client for '%s': %s", config.brokers, err)
	}

	return admin
}

func getTopicConfig(admin sarama.ClusterAdmin, topic, names string) ([]sarama.ConfigEntry, error) {
	var cn []string
	if names == "" {
		cn = nil
	} else {
		cn = strings.Split(names, ",")
	}

	resource := sarama.ConfigResource{
		Type:        sarama.TopicResource,
		Name:        topic,
		ConfigNames: cn,
	}

	entries, err := admin.DescribeConfig(resource)
	if err != nil {
		return nil, fmt.Errorf("error while getting configuration for topic '%s': %s", config.topic, err)
	}

	return entries, nil
}

func getCmdHandler(cmd *cobra.Command, args []string) {
	admin := newClusterAdmin()
	defer admin.Close()

	topics, err := topicsFromConfig()
	if err != nil {
		log.Fatalf("topicsFromConfig() failed: %s", err)
	}

	for _, t := range topics {
		entries, err := getTopicConfig(admin, t, config.names)
		if err != nil {
			log.Fatalf("get cmd failes: %s", err)
		}

		fmt.Printf("Current '%s' config: {\n", t)
		for _, entry := range entries {
			fmt.Printf("\t%s: %s\n", entry.Name, entry.Value)
		}
		fmt.Println("}")
	}
}

func topicsCmdHandler(cmd *cobra.Command, args []string) {
	admin := newClusterAdmin()
	defer admin.Close()

	topicsInfo, err := admin.ListTopics()
	if err != nil {
		log.Fatalf("error while getting tolic list: %s", err)
	}

	for topicName, _ := range topicsInfo {
		fmt.Println(topicName)
	}
}

func setTopicConfig(admin sarama.ClusterAdmin, topic string, values map[string]*string) error {
	curEntries, err := getTopicConfig(admin, topic, "")
	if err != nil {
		return err
	}

	var newEntries = make(map[string]*string)
	for i, _ := range curEntries {
		newEntries[curEntries[i].Name] = &curEntries[i].Value
	}

	for k, v := range values {
		newEntries[k] = v
	}

	if config.dryRun {
		fmt.Printf("New '%s' config (not updated, dry run): {\n", topic)
		for k, v := range newEntries {
			fmt.Printf("\t%s: %s\n", k, *v)
		}
		fmt.Println("}")

		return nil
	}

	if err := admin.AlterConfig(sarama.TopicResource, topic, newEntries, false); err != nil {
		return fmt.Errorf("error while setting configuration for topic '%s': %s", topic, err)
	}

	curEntries, err = getTopicConfig(admin, topic, "")
	if err != nil {
		return err
	}

	if config.debug {
		fmt.Printf("The result '%s' config: {\n", topic)
		for _, e := range curEntries {
			fmt.Printf("\t%s: %s\n", e.Name, e.Value)
		}
		fmt.Println("}")
	}

	return nil
}

func setCmdHandler(cmd *cobra.Command, args []string) {
	if config.values == "" {
		return
	}

	fmt.Println("Going to update these values: {")
	newConfigValues := make(map[string]*string)
	for _, item := range strings.Split(config.values, ",") {
		kv := strings.Split(item, "=")
		newConfigValues[kv[0]] = &kv[1]

		fmt.Printf("\t%s: %s\n", kv[0], kv[1])
	}
	fmt.Println("}")

	topics, err := topicsFromConfig()
	if err != nil {
		log.Fatalf("topicsFromConfig() failed: %s", err)
	}

	admin := newClusterAdmin()
	defer admin.Close()

	for _, t := range topics {
		if !config.debug && !config.dryRun {
			fmt.Printf("Updating topic '%s' ...", t)
		}

		if err := setTopicConfig(admin, t, newConfigValues); err != nil {
			fmt.Fprintf(os.Stderr, "Skip topic '%s': %s", t, err)
		}

		if !config.debug && !config.dryRun {
			fmt.Println(" Done")
		}
	}
}
