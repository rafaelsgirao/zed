package revert

import (
	"errors"
	"flag"
	"fmt"

	"github.com/brimdata/zed/cli/commitflags"
	"github.com/brimdata/zed/cli/lakeflags"
	"github.com/brimdata/zed/cli/poolflags"
	"github.com/brimdata/zed/cmd/zed/root"
	"github.com/brimdata/zed/lakeparse"
	"github.com/brimdata/zed/pkg/charm"
)

var Cmd = &charm.Spec{
	Name:  "revert",
	Usage: "revert commit",
	Short: "revert reverses an old commit",
	Long: `
The revert command reverses the actions in a commit by applying the inverse
steps in a new commit to the tip of the indicated branch.  Any data loaded
in a reverted commit remains in the lake but no longer appears in the branch.
The new commit may recursively be reverted by an additional revert operation.
`,
	New: New,
}

type Command struct {
	*root.Command
	commitFlags commitflags.Flags
	poolFlags   poolflags.Flags
}

func New(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &Command{Command: parent.(*root.Command)}
	c.commitFlags.SetFlags(f)
	c.poolFlags.SetFlags(f)
	return c, nil
}

func (c *Command) Run(args []string) error {
	ctx, cleanup, err := c.Init()
	if err != nil {
		return err
	}
	defer cleanup()
	if len(args) != 1 {
		return errors.New("commit ID must be specified")
	}
	lake, err := c.LakeFlags.Open(ctx)
	if err != nil {
		return err
	}
	head, err := c.poolFlags.HEAD()
	if err != nil {
		return err
	}
	if head.Pool == "" {
		return lakeflags.ErrNoHEAD
	}
	if _, err := lakeparse.ParseID(head.Branch); err == nil {
		return errors.New("branch must be named")
	}
	commitID, err := lakeparse.ParseID(args[0])
	if err != nil {
		return err
	}
	revertID, err := lake.Revert(ctx, head.Pool, head.Branch, commitID, c.commitFlags.CommitMessage())
	if err != nil {
		return err
	}
	if !c.LakeFlags.Quiet {
		fmt.Printf("%q: %s reverted in %s\n", head.Branch, commitID, revertID)
	}
	return nil
}
