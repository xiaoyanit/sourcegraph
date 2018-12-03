package graphqlbackend

import (
	"context"
	"errors"
	"unicode/utf8"

	"github.com/sourcegraph/sourcegraph/cmd/frontend/db"
	"github.com/sourcegraph/sourcegraph/cmd/frontend/types"
	"github.com/sourcegraph/sourcegraph/pkg/api"
	"github.com/sourcegraph/sourcegraph/pkg/search"
	"github.com/sourcegraph/sourcegraph/pkg/search/backend"
	"github.com/sourcegraph/sourcegraph/pkg/search/query"
	log15 "gopkg.in/inconshreveable/log15.v2"
)

// This file contains the resolvers for hierarchical search. The new
// hierarchical search attempts to leave much more business logic out of the
// graphqlbackend, and instead make the resolvers more dumb.

type searcherResolver struct {
	search.Searcher
	query.Q
	*search.Options
}

func newSearcherResolver(qStr string) (*searcherResolver, error) {
	q, err := query.Parse(qStr)
	if err != nil {
		log15.Debug("graphql search failed to parse", "query", qStr, "error", err)
		return nil, err
	}
	return &searcherResolver{
		Searcher: &backend.Mock{
			Result: &search.Result{
				Files: []search.FileMatch{{
					Path: "README.md",
					Repository: search.Repository{
						Name:   "github.com/sourcegraph/go-langserver",
						Commit: "1fe24b2b3fd1f420474fb872d944d2a1a93ddf63",
					},
				}},
			},
		},
		Q:       q,
		Options: &search.Options{},
	}, nil
}

func (r *searcherResolver) Results(ctx context.Context) (*searchResultsResolver, error) {
	// 1. Scope the request to repositories
	opts := r.Options.ShallowCopy()
	opts.Repositories = []search.Repository{{Name: "github.com/sourcegraph/go-langserver", Commit: "1fe24b2b3fd1f420474fb872d944d2a1a93ddf63"}}

	// 2. Do the search
	result, err := r.Searcher.Search(ctx, r.Q, opts)
	if err != nil {
		return nil, err
	}

	// 3. To ship hierarchical search sooner we are using the old file match
	//    resolver. However, we should just be returning a resolver which is a
	//    light wrapper around a search.Result.
	return &searchResultsResolver{results: toSearchResultResolvers(result)}, nil
}

func (r *searcherResolver) Suggestions(ctx context.Context, args *searchSuggestionsArgs) ([]*searchSuggestionResolver, error) {
	return nil, errors.New("search suggestions not implemented")
}

func (r *searcherResolver) Stats(ctx context.Context) (stats *searchResultsStats, err error) {
	return nil, errors.New("search stats not implemented")
}

func toSearchResultResolvers(r *search.Result) []*searchResultResolver {
	results := make([]*searchResultResolver, 0, len(r.Files))

	repos := map[api.RepoName]*types.Repo{}

	for _, file := range r.Files {
		fileLimitHit := false
		lines := make([]*lineMatch, 0, len(file.LineMatches))
		for _, l := range file.LineMatches {
			offsets := make([][2]int32, len(l.LineFragments))
			for k, m := range l.LineFragments {
				offset := utf8.RuneCount(l.Line[:m.LineOffset])
				length := utf8.RuneCount(l.Line[m.LineOffset : m.LineOffset+m.MatchLength])
				offsets[k] = [2]int32{int32(offset), int32(length)}
			}
			lines = append(lines, &lineMatch{
				JPreview:          string(l.Line),
				JLineNumber:       int32(l.LineNumber - 1),
				JOffsetAndLengths: offsets,
			})
		}

		// TODO some sort of helper which looks up based on previously looked up repos.
		repo, ok := repos[file.Repository.Name]
		if !ok {
			var err error
			repo, err = db.Repos.GetByName(context.TODO(), file.Repository.Name)
			if err != nil {
				log15.Error("failed to get repo", "repo", file.Repository, "error", err)
				continue
			}
			repos[file.Repository.Name] = repo
		}

		results = append(results, &searchResultResolver{
			fileMatch: &fileMatchResolver{
				JPath:        file.Path,
				JLineMatches: lines,
				JLimitHit:    fileLimitHit,
				uri:          fileMatchURI(file.Repository.Name, string(file.Repository.Commit), file.Path),
				repo:         repo,
				commitID:     file.Repository.Commit,
			},
		})
	}

	return results
}
