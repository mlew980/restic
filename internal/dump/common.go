package dump

import (
	"context"
	"io"
	"path"
	"sync"

	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/walker"
	"github.com/restic/restic/internal/debug"

	"golang.org/x/sync/errgroup"
)

// dumper implements saving node data.
type dumper interface {
	io.Closer
	dumpNode(ctx context.Context, node *restic.Node, repo restic.Repository) error
}

// WriteDump will write the contents of the given tree to the given destination.
// It will loop over all nodes in the tree and dump them recursively.
type WriteDump func(ctx context.Context, repo restic.Repository, tree *restic.Tree, rootPath string, dst io.Writer) error

func writeDump(ctx context.Context, repo restic.Repository, tree *restic.Tree, rootPath string, dmp dumper, dst io.Writer) error {
	for _, rootNode := range tree.Nodes {
		rootNode.Path = rootPath
		err := dumpTree(ctx, repo, rootNode, rootPath, dmp)
		if err != nil {
			// ignore subsequent errors
			_ = dmp.Close()

			return err
		}
	}

	return dmp.Close()
}

func dumpTree(ctx context.Context, repo restic.Repository, rootNode *restic.Node, rootPath string, dmp dumper) error {
	rootNode.Path = path.Join(rootNode.Path, rootNode.Name)
	rootPath = rootNode.Path

	if err := dmp.dumpNode(ctx, rootNode, repo); err != nil {
		return err
	}

	// If this is no directory we are finished
	if !IsDir(rootNode) {
		return nil
	}

	err := walker.Walk(ctx, repo, *rootNode.Subtree, nil, func(_ restic.ID, nodepath string, node *restic.Node, err error) (bool, error) {
		if err != nil {
			return false, err
		}
		if node == nil {
			return false, nil
		}

		node.Path = path.Join(rootPath, nodepath)

		if IsFile(node) || IsLink(node) || IsDir(node) {
			err := dmp.dumpNode(ctx, node, repo)
			if err != nil {
				return false, err
			}
		}

		return false, nil
	})

	return err
}

const numNodeDataWorkers = 8

// GetNodeData will write the contents of the node to the given output.
func GetNodeData(ctx context.Context, output io.Writer, repo restic.Repository, node *restic.Node) error {
	
	type processJob struct {
		hash restic.ID
		index int
		blob []byte
	}

	wg, wgCtx := errgroup.WithContext(ctx)

	inputJobs := make(chan processJob)

	debug.Log("Create subprocess to populate inputJobs channel with jobs to do.")
	wg.Go(func() error {
		defer close(inputJobs)
		for index, id := range node.Content {
			var (
				buf []byte
			)
			select {
			case inputJobs <- processJob{ id, index, buf }:
			case <-wgCtx.Done():
				return wgCtx.Err()
			}
		}
		return nil
	})

	debug.Log("Create a channel for completed jobs to feed into")
	outputJobs := make(chan processJob)

	var downloadWG sync.WaitGroup
	downloader := func() error {
		defer downloadWG.Done()
		for processJobObj := range inputJobs {
			var (
				buf []byte
				err error
			)
			buf, err = repo.LoadBlob(ctx, restic.DataBlob, processJobObj.hash, buf)
			if err != nil {
				return err
			}

			select {
				case outputJobs <- processJob{ processJobObj.hash, processJobObj.index, buf }:
				case <-wgCtx.Done():
					return wgCtx.Err()
			}

			if err != nil {
				return errors.Wrap(err, "Write")
			}
		}
		return nil
	}

	debug.Log("Spawn %v seperate threads that would download blobs.", numNodeDataWorkers)
	downloadWG.Add(numNodeDataWorkers)
	for i := 0; i < numNodeDataWorkers; i++ {
		wg.Go(downloader)
	}

	debug.Log("Once all blobs have been downloaded close the outputJobs channel.")
	wg.Go(func() error {
		downloadWG.Wait()
		close(outputJobs)
		return nil
	})

	completedJobs := make(map[int]processJob)

	var mutex sync.Mutex

	debug.Log("Create seperate process which reads in outputJobs as they come in and inserts them into completedJobs in the correct order (added Mutex for thread safe operation).")
	wg.Go(func() error {
		for outJob := range outputJobs {
			mutex.Lock()
			completedJobs[outJob.index] = outJob
			mutex.Unlock()
		}
		return nil
	})

	debug.Log("Keep looping until the next expectant job is completed and we can write it to io.Writer.")
	
	currentIndex := 0

	for {

		mutex.Lock()

		_, exists := completedJobs[currentIndex]
		if exists {
			debug.Log("If item exists write it to io.Writer and delete it from the map.")
			output.Write(completedJobs[currentIndex].blob)
			delete(completedJobs, currentIndex)
			currentIndex++
		}

		mutex.Unlock()

		if(currentIndex == len(node.Content)){
			break
		}

	}

	return nil
}

// IsDir checks if the given node is a directory.
func IsDir(node *restic.Node) bool {
	return node.Type == "dir"
}

// IsLink checks if the given node as a link.
func IsLink(node *restic.Node) bool {
	return node.Type == "symlink"
}

// IsFile checks if the given node is a file.
func IsFile(node *restic.Node) bool {
	return node.Type == "file"
}
