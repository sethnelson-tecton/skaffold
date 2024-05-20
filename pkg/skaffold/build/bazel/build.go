/*
Copyright 2019 The Skaffold Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package bazel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/google/go-containerregistry/pkg/v1/tarball"

	"github.com/GoogleContainerTools/skaffold/v2/pkg/skaffold/docker"
	"github.com/GoogleContainerTools/skaffold/v2/pkg/skaffold/output"
	"github.com/GoogleContainerTools/skaffold/v2/pkg/skaffold/output/log"
	"github.com/GoogleContainerTools/skaffold/v2/pkg/skaffold/platform"
	"github.com/GoogleContainerTools/skaffold/v2/pkg/skaffold/schema/latest"
	"github.com/GoogleContainerTools/skaffold/v2/pkg/skaffold/util"
)

// Build builds an artifact with Bazel.
func (b *Builder) Build(ctx context.Context, out io.Writer, artifact *latest.Artifact, tag string, matcher platform.Matcher) (string, error) {
	// TODO: Implement building multi-platform images
	if matcher.IsMultiPlatform() {
		log.Entry(ctx).Warnf("multiple target platforms %q found for artifact %q. Skaffold doesn't yet support multi-platform builds for the bazel builder. Consider specifying a single target platform explicitly. See https://skaffold.dev/docs/pipeline-stages/builders/#cross-platform-build-support", matcher.String(), artifact.ImageName)
	}

	a := artifact.ArtifactType.BazelArtifact

	tarPath, err := b.buildTar(ctx, out, artifact.Workspace, a, matcher)
	if err != nil {
		return "", err
	}

	if b.pushImages {
		return docker.Push(tarPath, tag, b.cfg, nil)
	}
	return b.loadImage(ctx, out, tarPath, tag)
}

func (b *Builder) SupportedPlatforms() platform.Matcher { return platform.All }

func bazelPlatform(matcher platform.Matcher, platformMappings []latest.BazelPlatformMapping) string {
	for _, mapping := range platformMappings {
		m, err := platform.Parse([]string{mapping.Platform})
		if err == nil {
			if matcher.Intersect(m).IsNotEmpty() {
				// TODO: Implement building multi-platform images
				return mapping.BazelPlatformTarget
			}
		}
	}
	return ""
}

func (b *Builder) buildTar(ctx context.Context, out io.Writer, workspace string, a *latest.BazelArtifact, matcher platform.Matcher) (string, error) {
	artifacts, err := b.buildTars(ctx, out, workspace, []*latest.BazelArtifact{a}, matcher)
	if err != nil {
		return "", nil
	}
	return artifacts[a], nil
}

func (b *Builder) buildTars(ctx context.Context, out io.Writer, workspace string, as []*latest.BazelArtifact, matcher platform.Matcher) (map[*latest.BazelArtifact]string, error) {
	buildTargets := make([]string, len(as))
	for i, a := range as {
		if !strings.HasSuffix(a.BuildTarget, ".tar") {
			return nil, errors.New("the bazel build target should end with .tar, see https://github.com/bazelbuild/rules_docker#using-with-docker-locally")
		}
		buildTargets[i] = a.BuildTarget
	}

	// We trust that all of these artifacts use the same args, because that is part of the batch cache key.
	// Ditto for platform mapping
	buildArgs := as[0].BuildArgs
	platformMappings := as[0].PlatformMappings
	args := []string{"build"}
	args = append(args, buildArgs...)
	args = append(args, buildTargets...)

	if bazelPlatform := bazelPlatform(matcher, platformMappings); bazelPlatform != "" {
		args = append(args, fmt.Sprintf("--platforms=%s", bazelPlatform))
	}

	if output.IsColorable(out) {
		args = append(args, "--color=yes")
	} else {
		args = append(args, "--color=no")
	}

	// FIXME: is it possible to apply b.skipTests?
	cmd := exec.CommandContext(ctx, "bazel", args...)
	cmd.Dir = workspace
	cmd.Stdout = out
	cmd.Stderr = out
	if err := util.RunCmd(ctx, cmd); err != nil {
		return nil, fmt.Errorf("running command: %w", err)
	}

	tarPaths := make(map[*latest.BazelArtifact]string, len(as))
	for _, a := range as {
		tarPath, err := bazelTarPath(ctx, workspace, a)
		if err != nil {
			return nil, fmt.Errorf("getting bazel tar path: %w", err)
		}
		tarPaths[a] = tarPath
	}
	return tarPaths, nil
}

func (b *Builder) loadImage(ctx context.Context, out io.Writer, tarPath string, tag string) (string, error) {
	manifest, err := tarball.LoadManifest(func() (io.ReadCloser, error) {
		return os.Open(tarPath)
	})

	if err != nil {
		return "", fmt.Errorf("loading manifest from tarball failed: %w", err)
	}

	imageTar, err := os.Open(tarPath)
	if err != nil {
		return "", fmt.Errorf("opening image tarball: %w", err)
	}
	defer imageTar.Close()

	bazelTag := manifest[0].RepoTags[0]
	imageID, err := b.localDocker.Load(ctx, out, imageTar, bazelTag)
	if err != nil {
		return "", fmt.Errorf("loading image into docker daemon: %w", err)
	}

	if err := b.localDocker.Tag(ctx, imageID, tag); err != nil {
		return "", fmt.Errorf("tagging the image: %w", err)
	}

	return imageID, nil
}

func bazelTarPath(ctx context.Context, workspace string, a *latest.BazelArtifact) (string, error) {
	args := []string{
		"cquery",
		a.BuildTarget,
		"--output",
		"starlark",
		// Bazel docker .tar output targets have a single output file, which is
		// the path to the image tar.
		"--starlark:expr",
		"target.files.to_list()[0].path",
	}
	args = append(args, a.BuildArgs...)

	cmd := exec.CommandContext(ctx, "bazel", args...)
	cmd.Dir = workspace

	buf, err := util.RunCmdOut(ctx, cmd)
	if err != nil {
		return "", err
	}

	targetPath := strings.TrimSpace(string(buf))

	cmd = exec.CommandContext(ctx, "bazel", "info", "execution_root")
	cmd.Dir = workspace

	buf, err = util.RunCmdOut(ctx, cmd)
	if err != nil {
		return "", err
	}

	execRoot := strings.TrimSpace(string(buf))

	return filepath.Join(execRoot, targetPath), nil
}

func (b *Builder) BuildMultiple(ctx context.Context, out io.Writer, as []*latest.Artifact, tags map[*latest.Artifact]string, platforms platform.Matcher) (map[*latest.Artifact]string, error) {
	// We include Workspace in the batch key, so it will always be the same here for all artifacts.
	bazelArtifacts := make([]*latest.BazelArtifact, len(as))
	for i, a := range as {
		bazelArtifacts[i] = a.BazelArtifact
	}

	tarPaths, err := b.buildTars(ctx, out, as[0].Workspace, bazelArtifacts, platforms)
	if err != nil {
		return nil, err
	}

	digests := make(map[*latest.Artifact]string, len(as))

	for _, a := range as {
		var digest string
		var err error
		if b.pushImages {
			digest, err = docker.Push(tarPaths[a.BazelArtifact], tags[a], b.cfg, nil)
		} else {
			digest, err = b.loadImage(ctx, out, tarPaths[a.BazelArtifact], tags[a])
		}
		if err != nil {
			return nil, err
		}
		digests[a] = digest
	}
	return digests, nil
}

func (*Builder) ComputeBatchKey(ctx context.Context, a *latest.Artifact, platforms platform.Matcher) string {
	sorted := make([]string, len(a.BazelArtifact.BuildArgs))
	copy(sorted, a.BazelArtifact.BuildArgs)
	sort.Strings(sorted)

	// TODO: Implement building multi-platform images
	if platforms.IsMultiPlatform() {
		log.Entry(ctx).Warnf("multiple target platforms %q found for artifact %q. Skaffold doesn't yet support multi-platform builds for the bazel builder. Consider specifying a single target platform explicitly. See https://skaffold.dev/docs/pipeline-stages/builders/#cross-platform-build-support", platforms.String(), a.ImageName)
	}
	bazelPlatform := bazelPlatform(platforms, a.BazelArtifact.PlatformMappings)
	return strings.Join(sorted, ",") + "%%" + a.Workspace + "%%" + bazelPlatform

}
