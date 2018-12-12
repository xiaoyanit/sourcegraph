package graphqlbackend

import (
	"context"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/pkg/errors"
	sgbackend "github.com/sourcegraph/sourcegraph/cmd/frontend/backend"
	"github.com/sourcegraph/sourcegraph/cmd/frontend/db"
	frontendsearch "github.com/sourcegraph/sourcegraph/cmd/frontend/internal/pkg/search"
	"github.com/sourcegraph/sourcegraph/cmd/frontend/types"
	"github.com/sourcegraph/sourcegraph/pkg/api"
	"github.com/sourcegraph/sourcegraph/pkg/gitserver"
	"github.com/sourcegraph/sourcegraph/pkg/search"
	"github.com/sourcegraph/sourcegraph/pkg/search/backend"
	"github.com/sourcegraph/sourcegraph/pkg/search/query"
	"github.com/sourcegraph/sourcegraph/pkg/vcs/git"
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
		Searcher: &backend.Text{
			Index: &backend.Zoekt{
				// TODO this can be nil
				Client: zoektCl,
			},
			Fallback: &backend.TextJIT{
				// TODO this can be nil
				Endpoints: searcherURLs,
				Resolve: func(ctx context.Context, name api.RepoName, spec string) (api.CommitID, error) {
					// Do not trigger a repo-updater lookup (e.g.,
					// backend.{GitRepo,Repos.ResolveRev}) because that would
					// slow this operation down by a lot (if we're looping
					// over many repos). This means that it'll fail if a repo
					// is not on gitserver.
					return git.ResolveRevision(ctx, gitserver.Repo{Name: name}, nil, spec, &git.ResolveRevisionOptions{NoEnsureRevision: true})
				},
			},
		},
		Q:       q,
		Options: &search.Options{},
	}, nil
}

func (r *searcherResolver) Results(ctx context.Context) (*searchResultsResolver, error) {
	sCtx := &searchContext{}
	start := time.Now()

	// 1. Scope the request to repositories
	dbQ, err := frontendsearch.RepoQuery(r.Q)
	if err != nil {
		return nil, err
	}
	maxRepoListSize := maxReposToSearch()
	repos, err := sgbackend.Repos.List(ctx, db.ReposListOptions{
		Enabled:      true,
		PatternQuery: dbQ,
		LimitOffset:  &db.LimitOffset{Limit: maxRepoListSize + 1}, // TODO check if we hit repo list size limit
		// TODO forks and archived
	})
	if err != nil {
		return nil, err
	}
	sCtx.CacheRepo(repos...)
	opts := r.Options.ShallowCopy()
	opts.Repositories = make([]api.RepoName, len(repos))
	for i := range repos {
		opts.Repositories[i] = repos[i].Name
	}

	// 2. Do the search
	result, err := r.Searcher.Search(ctx, r.Q, opts)
	if err != nil {
		return nil, err
	}

	// 3. To ship hierarchical search sooner we are using the old file match
	//    resolver. However, we should just be returning a resolver which is a
	//    light wrapper around a search.Result.
	results, err := toSearchResultResolvers(ctx, sCtx, result)
	if err != nil {
		return nil, err
	}
	common, err := toSearchResultsCommon(ctx, sCtx, opts, result)
	if err != nil {
		return nil, err
	}
	return &searchResultsResolver{
		results:             results,
		searchResultsCommon: *common,
		start:               start,
	}, nil
}

func (r *searcherResolver) Suggestions(ctx context.Context, args *searchSuggestionsArgs) ([]*searchSuggestionResolver, error) {
	return nil, errors.New("search suggestions not implemented")
}

func (r *searcherResolver) Stats(ctx context.Context) (stats *searchResultsStats, err error) {
	return nil, errors.New("search stats not implemented")
}

func toSearchResultResolvers(ctx context.Context, sCtx *searchContext, r *search.Result) ([]*searchResultResolver, error) {
	results := make([]*searchResultResolver, 0, len(r.Files))

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

		repo, err := sCtx.GetRepo(ctx, file.Repository.Name)
		if err != nil {
			return nil, err
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

	return results, nil
}

func toSearchResultsCommon(ctx context.Context, sCtx *searchContext, opts *search.Options, r *search.Result) (*searchResultsCommon, error) {
	var (
		repos    = map[api.RepoName]struct{}{}
		searched = map[api.RepoName]struct{}{}
		indexed  = map[api.RepoName]struct{}{}
		cloning  = map[api.RepoName]struct{}{}
		missing  = map[api.RepoName]struct{}{}
		partial  = map[api.RepoName]struct{}{}
		timedout = map[api.RepoName]struct{}{}
	)
	for _, s := range r.Stats.Status {
		repos[s.Repository.Name] = struct{}{}
		if s.Source == backend.SourceZoekt {
			indexed[s.Repository.Name] = struct{}{}
		}

		switch s.Status {
		case search.RepositoryStatusSearched:
			searched[s.Repository.Name] = struct{}{}

		case search.RepositoryStatusLimitHit:
			searched[s.Repository.Name] = struct{}{}
			partial[s.Repository.Name] = struct{}{}

		case search.RepositoryStatusTimedOut:
			timedout[s.Repository.Name] = struct{}{}

		case search.RepositoryStatusCloning:
			cloning[s.Repository.Name] = struct{}{}

		case search.RepositoryStatusMissing:
			missing[s.Repository.Name] = struct{}{}

		case search.RepositoryStatusError:
			return nil, errors.Errorf("error repository status: %v", s)

		default:
			return nil, errors.Errorf("unknown repository status: %v", s)
		}
	}

	unavailable := map[search.Source]bool{}
	for _, u := range r.Stats.Unavailable {
		unavailable[u] = true
	}

	var retErr error
	list := func(m map[api.RepoName]struct{}) []*types.Repo {
		repos := make([]*types.Repo, 0, len(m))
		for name := range m {
			repo, err := sCtx.GetRepo(ctx, name)
			if err != nil {
				retErr = err
				continue
			}
			repos = append(repos, repo)
		}
		return repos
	}

	common := &searchResultsCommon{
		maxResultsCount:  int32(opts.TotalMaxMatchCount),
		resultCount:      int32(r.Stats.MatchCount),
		indexUnavailable: unavailable[backend.SourceZoekt],

		searched: list(searched),
		indexed:  list(indexed),
		cloning:  list(cloning),
		missing:  list(missing),
		timedout: list(timedout),
		partial:  partial,
	}
	if retErr != nil {
		return nil, retErr
	}
	return common, nil
}

// searchContext is used to reduce duplicate DB and gitserver calls.
type searchContext struct {
	mu    sync.Mutex
	repos map[api.RepoName]*types.Repo
}

func (s *searchContext) GetRepo(ctx context.Context, name api.RepoName) (*types.Repo, error) {
	s.mu.Lock()
	if s.repos == nil {
		s.repos = map[api.RepoName]*types.Repo{}
	}
	r, ok := s.repos[name]
	s.mu.Unlock()
	if ok {
		return r, nil
	}
	r, err := db.Repos.GetByName(ctx, name)
	if err != nil {
		return nil, err
	}
	s.CacheRepo(r)
	return r, nil
}

func (s *searchContext) CacheRepo(repos ...*types.Repo) {
	s.mu.Lock()
	if s.repos == nil {
		s.repos = map[api.RepoName]*types.Repo{}
	}
	for _, r := range repos {
		s.repos[r.Name] = r
	}
	s.mu.Unlock()
}
