package main

import (
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
	brokers string
	topic   string
	names   string
	values  string
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&config.brokers, "brokers", "b", "", "brokers (i.e. \"kafka1.mlan:9092,kafka2.mlan:9092\")")

	getCmd.Flags().StringVarP(&config.names, "names", "n", "", "config names (i.e. \"compression.type,min.insync.replicas\")")
	getCmd.Flags().StringVarP(&config.topic, "topic", "t", "", "topic (i.e. \"meetmaker-bg\")")

	setCmd.Flags().StringVarP(&config.values, "values", "v", "", "config names and vales (i.e. \"retention.ms=604800001,min.insync.replicas=3\")")
	setCmd.Flags().StringVarP(&config.topic, "topic", "t", "", "topic (i.e. \"meetmaker-bg\")")

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

func getCmdHandler(cmd *cobra.Command, args []string) {
	admin := newClusterAdmin()

	var cn []string
	if config.names == "" {
		cn = nil
	} else {
		cn = strings.Split(config.names, ",")
	}

	resource := sarama.ConfigResource{
		Type:        sarama.TopicResource,
		Name:        config.topic,
		ConfigNames: cn,
	}
	entries, err := admin.DescribeConfig(resource)
	if err != nil {
		log.Fatalf("error while getting configuration for topic '%s': %s", config.topic, err)
	}

	for _, entry := range entries {
		fmt.Printf("%s: %s\n", entry.Name, entry.Value)
	}
}

func topicsCmdHandler(cmd *cobra.Command, args []string) {
	admin := newClusterAdmin()

	topicsInfo, err := admin.ListTopics()
	if err != nil {
		log.Fatalf("error while getting tolic list: %s", err)
	}

	for topicName, _ := range topicsInfo {
		fmt.Println(topicName)
	}
}

func setCmdHandler(cmd *cobra.Command, args []string) {
	admin := newClusterAdmin()

	if config.values == "" {
		return
	}

	var entries = make(map[string]*string)

	for _, item := range strings.Split(config.values, ",") {
		kv := strings.Split(item, "=")
		entries[kv[0]] = &kv[1]
	}

	fmt.Println("Will set these values:")
	for k, v := range entries {
		fmt.Printf("%s: %s\n", k, *v)
	}

	if err := admin.AlterConfig(sarama.TopicResource, config.topic, entries, false); err != nil {
		log.Fatalf("error while setting configuration for topic '%s': %s", config.topic, err)
	}
}
