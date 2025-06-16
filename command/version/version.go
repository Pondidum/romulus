package version

import (
	"context"
	"fmt"
	"openretriever/config"

	"github.com/spf13/pflag"
)

var (
	GitCommit  string
	Prerelease = "dev"
)

func VersionNumber() string {
	if GitCommit == "" {
		return "local"
	}

	version := GitCommit[0:7]

	if Prerelease != "" {
		version = fmt.Sprintf("%s-%s", version, Prerelease)
	}

	return version
}

func NewVersionCommand() *VersionCommand {
	return &VersionCommand{}
}

type VersionCommand struct {
}

func (c *VersionCommand) Synopsis() string {
	return "prints the version number"
}

func (c *VersionCommand) Flags() *pflag.FlagSet {
	flags := pflag.NewFlagSet("version", pflag.ContinueOnError)
	return flags
}

func (c *VersionCommand) Execute(ctx context.Context, config *config.Config, args []string) error {
	fmt.Println(VersionNumber())
	return nil
}
