package bazel

import (
	"context"
	"fmt"
	"github.com/GoogleContainerTools/skaffold/v2/pkg/skaffold/output/log"
	"github.com/GoogleContainerTools/skaffold/v2/pkg/skaffold/platform"
	"github.com/GoogleContainerTools/skaffold/v2/pkg/skaffold/schema/latest"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

type multiJarBuilder func(ctx context.Context, out io.Writer, workspace string, as []*latest.BazelArtifact, matcher platform.Matcher) (map[*latest.BazelArtifact]string, error)

type batchingBuilder struct {
	builder   multiJarBuilder
	batchSize time.Duration
	builds    map[string]*MultiArtifactBuild
	mu        sync.Mutex
}

var singletonBatchingBuilder = &batchingBuilder{
	builder:   buildTars,
	batchSize: 250 * time.Millisecond,
	builds:    make(map[string]*MultiArtifactBuild),
}

func computeBatchKey(ctx context.Context, a *latest.BazelArtifact, workspace string, platforms platform.Matcher) string {
	sorted := make([]string, len(a.BuildArgs))
	copy(sorted, a.BuildArgs)
	sort.Strings(sorted)

	// TODO: Implement building multi-platform images
	if platforms.IsMultiPlatform() {
		log.Entry(ctx).Warnf("multiple target platforms %q found for artifact %q. Skaffold doesn't yet support multi-platform builds for the bazel builder. Consider specifying a single target platform explicitly. See https://skaffold.dev/docs/pipeline-stages/builders/#cross-platform-build-support", platforms.String(), a.BuildTarget)
	}
	bazelPlatform := bazelPlatform(platforms, a.PlatformMappings)
	return strings.Join(sorted, ",") + "%%" + workspace + "%%" + bazelPlatform
}

func (b *batchingBuilder) getBuild(ctx context.Context, out io.Writer, a *latest.Artifact, platforms platform.Matcher) *MultiArtifactBuild {
	flags := a.BazelArtifact.BuildArgs
	target := a.BazelArtifact.BuildTarget
	workspace := a.Workspace

	batchKey := computeBatchKey(ctx, a.BazelArtifact, a.Workspace, platforms)
	log.Entry(ctx).Warnf("Batch Key for %s: %s", a.ImageName, batchKey)
	b.mu.Lock()
	defer b.mu.Unlock()
	if val, ok := b.builds[batchKey]; ok {
		val.mu.Lock()
		running := val.running
		val.mu.Unlock()
		if running {
			//  Builder for this key is running - kick off a new one.
			b.builds[batchKey] = b.newBuild(ctx, out, workspace, a, platforms)
		}
		val.artifacts = append(val.artifacts, a)
		return val
	} else {
		log.Entry(ctx).Warnf("New build for target (didn't match %d builds): %v; args: %v", len(b.builds), target, flags)
		b.builds[batchKey] = b.newBuild(ctx, out, workspace, a, platforms)
	}

	build := b.builds[batchKey]
	if build.workspace != workspace {
		panic(fmt.Sprintf("Workspace don't match: %s vs %s", build, workspace))
	}
	return build
}

func (b *batchingBuilder) newBuild(ctx context.Context, out io.Writer, workspace string, a *latest.Artifact, platforms platform.Matcher) *MultiArtifactBuild {
	mab := MultiArtifactBuild{
		ctx:       ctx,
		artifacts: []*latest.Artifact{a},
		platforms: platforms,
		out:       out,
		workspace: workspace,
		doneCond:  make(chan bool),
	}
	go mab.run(b.builder, b.batchSize)
	return &mab
}

func (b *batchingBuilder) build(ctx context.Context, out io.Writer, a *latest.Artifact, platforms platform.Matcher) (string, error) {
	build := b.getBuild(ctx, out, a, platforms)
	<-build.doneCond
	if build.err != nil {
		return "", build.err
	}
	return build.digest[a.BazelArtifact], nil
}

type MultiArtifactBuild struct {
	ctx       context.Context
	artifacts []*latest.Artifact
	platforms platform.Matcher
	workspace string
	mu        sync.Mutex
	doneCond  chan bool
	running   bool
	out       io.Writer
	digest    map[*latest.BazelArtifact]string
	err       error
}

func (b *MultiArtifactBuild) run(maj multiJarBuilder, batchSize time.Duration) {
	time.Sleep(batchSize)
	b.mu.Lock()
	b.running = true
	b.mu.Unlock()

	bazelArtifacts := make([]*latest.BazelArtifact, len(b.artifacts))
	for i, a := range b.artifacts {
		bazelArtifacts[i] = a.BazelArtifact
	}
	b.digest, b.err = maj(b.ctx, b.out, b.workspace, bazelArtifacts, b.platforms)
	b.doneCond <- true
}
