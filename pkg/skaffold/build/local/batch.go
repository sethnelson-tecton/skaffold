package local

import (
	"context"
	"fmt"
	"github.com/GoogleContainerTools/skaffold/v2/pkg/skaffold/output/log"
	"github.com/GoogleContainerTools/skaffold/v2/pkg/skaffold/platform"
	"github.com/GoogleContainerTools/skaffold/v2/pkg/skaffold/schema/latest"
	"io"
	"sync"
	"time"
)

type multiArtifactBuilder interface {
	artifactBuilder
	BuildMultiple(ctx context.Context, out io.Writer, a []*latest.Artifact, tags map[*latest.Artifact]string, platforms platform.Matcher) (map[*latest.Artifact]string, error)
	ComputeBatchKey(ctx context.Context, a *latest.Artifact, platforms platform.Matcher) string // does this need more?
}

type MultiArtifactBuild struct {
	ctx       context.Context
	artifacts []*latest.Artifact
	tags      map[*latest.Artifact]string
	platforms platform.Matcher
	workspace string
	mu        sync.Mutex
	doneCond  chan bool
	running   bool
	out       io.Writer
	digest    map[*latest.Artifact]string
	err       error
}

type BatchingBuilder struct {
	builder   multiArtifactBuilder
	batchSize time.Duration

	builds map[string]*MultiArtifactBuild
	mu     sync.Mutex
}

func (b *BatchingBuilder) getBuild(ctx context.Context, out io.Writer, a *latest.Artifact, tag string, platforms platform.Matcher) *MultiArtifactBuild {
	flags := a.BazelArtifact.BuildArgs
	target := a.BazelArtifact.BuildTarget
	workspace := a.Workspace

	batchKey := b.builder.ComputeBatchKey(ctx, a, platforms)
	log.Entry(ctx).Warnf("Batch Key for %s: %s", a.ImageName, batchKey)
	b.mu.Lock()
	defer b.mu.Unlock()
	if val, ok := b.builds[batchKey]; ok {
		val.mu.Lock()
		running := val.running
		val.mu.Unlock()
		if running {
			//  Builder for this key is running - kick off a new one.
			b.builds[batchKey] = b.newBuild(ctx, out, workspace, a, tag, platforms)
		}
		val.artifacts = append(val.artifacts, a)
		val.tags[a] = tag
		return val
	} else {
		log.Entry(ctx).Warnf("New build for target (didn't match %d builds): %v; args: %v", len(b.builds), target, flags)
		b.builds[batchKey] = b.newBuild(ctx, out, workspace, a, tag, platforms)
	}

	build := b.builds[batchKey]
	if build.workspace != workspace {
		panic(fmt.Sprintf("Workspace don't match: %s vs %s", build, workspace))
	}
	return build
}

func (b *MultiArtifactBuild) run(mab multiArtifactBuilder, batchSize time.Duration) {
	time.Sleep(batchSize)
	b.mu.Lock()
	b.running = true
	b.mu.Unlock()

	b.digest, b.err = mab.BuildMultiple(b.ctx, b.out, b.artifacts, b.tags, b.platforms)
	b.doneCond <- true
}

func (b *BatchingBuilder) newBuild(ctx context.Context, out io.Writer, workspace string, a *latest.Artifact, tag string, platforms platform.Matcher) *MultiArtifactBuild {
	mab := MultiArtifactBuild{
		ctx:       ctx,
		artifacts: []*latest.Artifact{a},
		tags:      map[*latest.Artifact]string{a: tag},
		platforms: platforms,
		out:       out,
		workspace: workspace,
		doneCond:  make(chan bool),
	}
	go mab.run(b.builder, b.batchSize)
	return &mab
}

func (b *BatchingBuilder) Build(ctx context.Context, out io.Writer, a *latest.Artifact, tag string, platforms platform.Matcher) (string, error) {
	build := b.getBuild(ctx, out, a, tag, platforms)
	<-build.doneCond
	if build.err != nil {
		return "", build.err
	}
	return build.digest[a], nil

}
func (b *BatchingBuilder) SupportedPlatforms() platform.Matcher {
	return b.builder.SupportedPlatforms()
}

func NewBatchingBuilder(builder multiArtifactBuilder, batchSize time.Duration) *BatchingBuilder {
	return &BatchingBuilder{
		builder:   builder,
		batchSize: batchSize,
		builds:    make(map[string]*MultiArtifactBuild),
	}
}
