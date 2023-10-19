package service

import (
	"errors"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/api"
	"github.com/brimdata/zed/api/queryio"
	"github.com/brimdata/zed/compiler"
	"github.com/brimdata/zed/lake"
	lakeapi "github.com/brimdata/zed/lake/api"
	"github.com/brimdata/zed/lake/commits"
	"github.com/brimdata/zed/lake/index"
	"github.com/brimdata/zed/lake/journal"
	"github.com/brimdata/zed/lakeparse"
	"github.com/brimdata/zed/runtime"
	"github.com/brimdata/zed/runtime/exec"
	"github.com/brimdata/zed/runtime/op"
	"github.com/brimdata/zed/service/auth"
	"github.com/brimdata/zed/service/srverr"
	"github.com/brimdata/zed/zio"
	"github.com/brimdata/zed/zio/anyio"
	"github.com/brimdata/zed/zio/csvio"
	"github.com/brimdata/zed/zio/zngio"
	"github.com/segmentio/ksuid"
	"go.uber.org/zap"
)

func handleQuery(c *Core, w *ResponseWriter, r *Request) {
	const queryStatsInterval = time.Second
	var req api.QueryRequest
	if !r.Unmarshal(w, &req) {
		return
	}
	r.Logger.Debug("Running Query", zap.String("query", req.Query))
	ctrl, ok := r.BoolFromQuery(w, "ctrl")
	if !ok {
		return
	}
	// A note on error handling here.  If we get an error setting up
	// before the query starts to run, we call w.Error() and return
	// an HTTP status error and a JSON formatted error.  If the query
	// begins running then we encounter an error, we return an HTTP
	// status OK (triggered as we start to write to the HTTP response body)
	// and return the error as an embedded ZNG control message.
	// The client must look at the return code and interpret the result
	// accordingly and when it sees a ZNG error after underway,
	// the error should be relay that to the caller/user.
	query, err := c.compiler.Parse(req.Query)
	if err != nil {
		w.Error(srverr.ErrInvalid(err))
		return
	}
	flowgraph, err := runtime.CompileLakeQuery(r.Context(), zed.NewContext(), c.compiler, query, &req.Head, r.Logger)
	if err != nil {
		w.Error(srverr.ErrInvalid(err))
		return
	}
	flusher, _ := w.ResponseWriter.(http.Flusher)
	writer, err := queryio.NewWriter(zio.NopCloser(w), w.Format, flusher, ctrl)
	if err != nil {
		w.Error(srverr.ErrInvalid(err))
		return
	}
	// Once we defer writer.Close() are going to write ZNG to the HTTP
	// response body and for errors after this point, we must call
	// writer.WriterError() instead of w.Error().
	defer writer.Close()
	// Launch query status which will report and runtime errors (i.e., system
	// errors that occur after the OK header has been sent) to the query status
	// endpoint.
	status := c.newQueryStatus(r)
	defer status.Done()
	handleError := func(err error) {
		writer.WriteError(err)
		status.setError(err)
	}
	results := make(chan op.Result)
	go func() {
		for {
			batch, err := flowgraph.Pull(false)
			results <- op.Result{Batch: batch, Err: err}
			if batch == nil || err != nil {
				return
			}
		}
	}()
	timer := time.NewTicker(queryStatsInterval)
	defer timer.Stop()
	meter := flowgraph.Meter()
	for {
		select {
		case <-timer.C:
			if err := writer.WriteProgress(meter.Progress()); err != nil {
				w.Logger.Warn("Error writing progress", zap.Error(err))
				handleError(err)
				return
			}
		case r := <-results:
			batch, err := r.Batch, r.Err
			if err != nil {
				if !errors.Is(err, journal.ErrEmpty) {
					w.Logger.Warn("Error pulling batch", zap.Error(err))
					handleError(err)
				}
				return
			}
			if batch == nil {
				if err := writer.WriteProgress(meter.Progress()); err != nil {
					w.Logger.Warn("Error writing progress", zap.Error(err))
					handleError(err)
					return
				}
				if batch == nil {
					return
				}
			}
			if len(batch.Values()) == 0 {
				if eoc, ok := batch.(*op.EndOfChannel); ok {
					if err := writer.WhiteChannelEnd(int(*eoc)); err != nil {
						w.Logger.Warn("Error writing channel end", zap.Error(err))
						handleError(err)
						return
					}
				}
				continue
			}
			var cid int
			batch, cid = op.Unwrap(batch)
			if err := writer.WriteBatch(cid, batch); err != nil {
				w.Logger.Warn("Error writing batch", zap.Error(err))
				handleError(err)
				return
			}
		}
	}
}

func handleQueryStatus(c *Core, w *ResponseWriter, r *Request) {
	id, ok := r.StringFromPath(w, "requestID")
	if !ok {
		return
	}
	c.runningQueriesMu.Lock()
	q, ok := c.runningQueries[id]
	c.runningQueriesMu.Unlock()
	if !ok {
		w.Error(srverr.ErrInvalid("query not found"))
		return
	}
	q.wg.Wait()
	w.Respond(http.StatusOK, api.QueryError{Error: q.error})
}

func handleBranchGet(c *Core, w *ResponseWriter, r *Request) {
	pool, ok := r.StringFromPath(w, "pool")
	if !ok {
		return
	}
	branchName, ok := r.StringFromPath(w, "branch")
	if !ok {
		return
	}
	commit, err := c.lakeapi.CommitObject(r.Context(), pool, branchName)
	if err != nil {
		w.Error(err)
		return
	}
	w.Respond(http.StatusOK, api.CommitResponse{Commit: commit})
}

func handlePoolStats(c *Core, w *ResponseWriter, r *Request) {
	id, ok := r.PoolID(w, c.lakeapi)
	if !ok {
		return
	}
	pool, err := c.root.OpenPool(r.Context(), id)
	if err != nil {
		w.Error(err)
		return
	}
	//XXX app uses this for key range... should handle this differently
	// at minimum on a per-branch basis and app needs to be branch aware
	// If branch not specified, API endpoints here should just assume "main".
	branch, err := pool.OpenBranchByName(r.Context(), "main")
	if err != nil {
		w.Error(err)
		return
	}
	snap, err := branch.Pool().Snapshot(r.Context(), branch.Commit)
	if err != nil {
		if errors.Is(err, journal.ErrEmpty) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Error(err)
		return
	}
	info, err := exec.GetPoolStats(r.Context(), pool, snap)
	if err != nil {
		w.Error(err)
		return
	}
	w.Respond(http.StatusOK, info)
}

func handlePoolPost(c *Core, w *ResponseWriter, r *Request) {
	var req api.PoolPostRequest
	if !r.Unmarshal(w, &req) {
		return
	}
	pool, err := c.root.CreatePool(r.Context(), req.Name, req.SortKey, req.SeekStride, req.Thresh)
	if err != nil {
		w.Error(err)
		return
	}
	meta, err := pool.Main(r.Context())
	if err != nil {
		w.Error(err)
		return
	}
	w.Respond(http.StatusOK, meta)
	c.publishEvent(w, "pool-new", api.EventPool{PoolID: pool.ID})
}

func handlePoolPut(c *Core, w *ResponseWriter, r *Request) {
	pool, id, ok := r.poolFromPathAndID(w, c.lakeapi)
	if !ok {
		return
	}
	var req api.PoolPutRequest
	if !r.Unmarshal(w, &req) {
		return
	}
	if err := c.lakeapi.RenamePool(r.Context(), pool, req.Name); err != nil {
		w.Error(err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
	c.publishEvent(w, "pool-update", api.EventPool{PoolID: id})
}

func handleBranchPost(c *Core, w *ResponseWriter, r *Request) {
	pool, id, ok := r.poolFromPathAndID(w, c.lakeapi)
	if !ok {
		return
	}
	var req api.BranchPostRequest
	if !r.Unmarshal(w, &req) {
		return
	}
	commit, err := lakeparse.ParseID(req.Commit)
	if err != nil {
		w.Error(srverr.ErrInvalid("invalid commit object: %s", req.Commit))
		return
	}
	if err := c.lakeapi.CreateBranch(r.Context(), pool, req.Name, commit); err != nil {
		w.Error(err)
		return
	}
	c.publishEvent(w, "branch-update", api.EventBranch{PoolID: id, Branch: req.Name})
}

func handleRevertPost(c *Core, w *ResponseWriter, r *Request) {
	pool, id, ok := r.poolFromPathAndID(w, c.lakeapi)
	if !ok {
		return
	}
	branch, ok := r.StringFromPath(w, "branch")
	if !ok {
		return
	}
	commit, ok := r.CommitID(w)
	if !ok {
		return
	}
	message, ok := r.decodeCommitMessage(w)
	if !ok {
		return
	}
	commit, err := c.lakeapi.Revert(r.Context(), pool, branch, commit, message)
	if err != nil {
		w.Error(err)
		return
	}
	w.Respond(http.StatusOK, api.CommitResponse{Commit: commit})
	c.publishEvent(w, "branch-commit", api.EventBranchCommit{
		CommitID: commit,
		PoolID:   id,
		Branch:   branch,
	})
}

func handleBranchMerge(c *Core, w *ResponseWriter, r *Request) {
	pool, id, ok := r.poolFromPathAndID(w, c.lakeapi)
	if !ok {
		return
	}
	parentBranch, ok := r.StringFromPath(w, "branch")
	if !ok {
		return
	}
	childBranch, ok := r.StringFromPath(w, "child")
	if !ok {
		return
	}
	message, ok := r.decodeCommitMessage(w)
	if !ok {
		return
	}
	commit, err := c.lakeapi.MergeBranch(r.Context(), pool, childBranch, parentBranch, message)
	if err != nil {
		w.Error(err)
		return
	}
	w.Respond(http.StatusOK, api.CommitResponse{Commit: commit})
	c.publishEvent(w, "branch-commit", api.EventBranchCommit{
		CommitID: commit,
		PoolID:   id,
		Branch:   childBranch,
		Parent:   parentBranch,
	})
}

func handlePoolDelete(c *Core, w *ResponseWriter, r *Request) {
	pool, id, ok := r.poolFromPathAndID(w, c.lakeapi)
	if !ok {
		return
	}
	if err := c.lakeapi.RemovePool(r.Context(), pool); err != nil {
		w.Error(err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
	c.publishEvent(w, "pool-delete", api.EventPool{PoolID: id})
}

func handleBranchDelete(c *Core, w *ResponseWriter, r *Request) {
	pool, id, ok := r.poolFromPathAndID(w, c.lakeapi)
	if !ok {
		return
	}
	branchName, ok := r.StringFromPath(w, "branch")
	if !ok {
		return
	}
	if err := c.lakeapi.RemoveBranch(r.Context(), pool, branchName); err != nil {
		w.Error(err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
	c.publishEvent(w, "branch-delete", api.EventBranch{PoolID: id, Branch: branchName})
}

func handleBranchLoad(c *Core, w *ResponseWriter, r *Request) {
	pool, id, ok := r.poolFromPathAndID(w, c.lakeapi)
	if !ok {
		return
	}
	branchName, ok := r.StringFromPath(w, "branch")
	if !ok {
		return
	}
	format, ok := r.format(w, "auto")
	if !ok {
		return
	}
	var csvDelim rune
	if s := r.URL.Query().Get("csv.delim"); s != "" {
		if len(s) != 1 {
			w.Error(srverr.ErrInvalid(`invalid query param "csv.delim": must be exactly one character`))
			return
		}
		csvDelim = rune(s[0])
	}
	message, ok := r.decodeCommitMessage(w)
	if !ok {
		return
	}
	reader, err := anyio.GzipReader(r.Body)
	if err != nil {
		w.Error(err)
		return
	}
	if format == "parquet" || format == "vng" {
		// These formats require a reader that implements io.ReaderAt and
		// io.Seeker.  Copy the reader to a temporary file and use that.
		//
		// TODO: Add a way to disable this or limit file size.
		f, err := os.CreateTemp("", "zed-serve-load-")
		if err != nil {
			w.Error(err)
			return
		}
		defer f.Close()
		defer os.Remove(f.Name())
		if _, err := io.Copy(f, reader); err != nil {
			w.Error(err)
			return
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			w.Error(err)
			return
		}
		reader = f
	}
	opts := anyio.ReaderOpts{
		Format: format,
		CSV:    csvio.ReaderOpts{Delim: csvDelim},
		// Force validation of ZNG when loading into the lake.
		ZNG: zngio.ReaderOpts{Validate: true},
	}
	zctx := zed.NewContext()
	zrc, err := anyio.NewReaderWithOpts(zctx, reader, opts)
	if err != nil {
		w.Error(srverr.ErrInvalid(err))
		return
	}
	defer zrc.Close()
	wr := &warningsReader{zrc, []string{}}
	kommit, err := c.lakeapi.Load(r.Context(), zctx, pool, branchName, wr, message)
	if err != nil {
		if errors.Is(err, commits.ErrEmptyTransaction) {
			err = srverr.ErrInvalid("no records in request")
		}
		if errors.Is(err, lake.ErrInvalidCommitMeta) {
			err = srverr.ErrInvalid("invalid commit metadata in request")
		}
		w.Error(err)
		return
	}
	w.Respond(http.StatusOK, api.CommitResponse{
		Warnings: wr.warnings,
		Commit:   kommit,
	})
	c.publishEvent(w, "branch-commit", api.EventBranchCommit{
		CommitID: kommit,
		PoolID:   id,
		Branch:   branchName,
	})
}

type warningsReader struct {
	zio.Reader
	warnings []string
}

func (w *warningsReader) Read() (*zed.Value, error) {
	val, err := w.Reader.Read()
	if err != nil {
		w.warnings = append(w.warnings, err.Error())
		return nil, nil
	}
	return val, nil
}

func handleCompact(c *Core, w *ResponseWriter, r *Request) {
	var req api.CompactRequest
	if !r.Unmarshal(w, &req) {
		return
	}
	pool, id, ok := r.poolFromPathAndID(w, c.lakeapi)
	if !ok {
		return
	}
	branch, ok := r.StringFromPath(w, "branch")
	if !ok {
		return
	}
	writeVectors, ok := r.BoolFromQuery(w, "vectors")
	if !ok {
		return
	}
	message, ok := r.decodeCommitMessage(w)
	if !ok {
		return
	}
	commit, err := c.lakeapi.Compact(r.Context(), pool, branch, req.ObjectIDs, writeVectors, message)
	if err != nil {
		w.Error(err)
		return
	}
	w.Respond(http.StatusOK, api.CommitResponse{Commit: commit})
	c.publishEvent(w, "branch-commit", api.EventBranchCommit{
		CommitID: commit,
		PoolID:   id,
		Branch:   branch,
	})
}

func handleDelete(c *Core, w *ResponseWriter, r *Request) {
	pool, id, ok := r.poolFromPathAndID(w, c.lakeapi)
	if !ok {
		return
	}
	branchName, ok := r.StringFromPath(w, "branch")
	if !ok {
		return
	}
	message, ok := r.decodeCommitMessage(w)
	if !ok {
		return
	}
	var payload api.DeleteRequest
	if !r.Unmarshal(w, &payload) {
		return
	}
	var commit ksuid.KSUID
	var err error
	if len(payload.ObjectIDs) > 0 {
		if payload.Where != "" {
			w.Error(srverr.ErrInvalid("object_ids and where cannot both be set"))
			return
		}
		var ids []ksuid.KSUID
		ids, err = lakeparse.ParseIDs(payload.ObjectIDs)
		if err != nil {
			w.Error(srverr.ErrInvalid(err))
			return
		}
		commit, err = c.lakeapi.Delete(r.Context(), pool, branchName, ids, message)
	} else {
		if payload.Where == "" {
			w.Error(srverr.ErrInvalid("either object_ids or where must be set"))
			return
		}
		commit, err = c.lakeapi.DeleteWhere(r.Context(), pool, branchName, payload.Where, message)
		if errors.Is(err, commits.ErrEmptyTransaction) ||
			errors.Is(err, &compiler.InvalidDeleteWhereQuery{}) {
			err = srverr.ErrInvalid(err)
		}
	}
	if err != nil {
		w.Error(err)
		return
	}
	w.Marshal(api.CommitResponse{Commit: commit})
	c.publishEvent(w, "branch-commit", api.EventBranchCommit{
		CommitID: commit,
		PoolID:   id,
		Branch:   branchName,
	})
}

func handleIndexRulesPost(c *Core, w *ResponseWriter, r *Request) {
	var body api.IndexRulesAddRequest
	if !r.Unmarshal(w, &body, index.RuleTypes...) {
		return
	}
	if err := c.root.AddIndexRules(r.Context(), body.Rules); err != nil {
		w.Error(err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func handleIndexRulesDelete(c *Core, w *ResponseWriter, r *Request) {
	var req api.IndexRulesDeleteRequest
	if !r.Unmarshal(w, &req) {
		return
	}
	ruleIDs, err := lakeparse.ParseIDs(req.RuleIDs)
	if err != nil {
		w.Error(srverr.ErrInvalid(err))
	}
	rules, err := c.root.DeleteIndexRules(r.Context(), ruleIDs)
	if err != nil {
		w.Error(err)
		return
	}
	w.Respond(http.StatusOK, api.IndexRulesDeleteResponse{Rules: rules})
}

func handleIndexApply(c *Core, w *ResponseWriter, r *Request) {
	pool, id, ok := r.poolFromPathAndID(w, c.lakeapi)
	if !ok {
		return
	}
	branchName, ok := r.StringFromPath(w, "branch")
	if !ok {
		return
	}
	var req api.IndexApplyRequest
	if !r.Unmarshal(w, &req) {
		return
	}
	ss, err := lakeparse.ParseIDs(req.Tags)
	if err != nil {
		w.Error(srverr.ErrInvalid(err))
		return
	}
	commit, err := c.lakeapi.ApplyIndexRules(r.Context(), req.Rules, pool, branchName, ss)
	if err != nil {
		w.Error(err)
		return
	}
	w.Respond(http.StatusOK, api.CommitResponse{Commit: commit})
	c.publishEvent(w, "branch-commit", api.EventBranchCommit{
		CommitID: commit,
		PoolID:   id,
		Branch:   branchName,
	})

}

func handleIndexUpdate(c *Core, w *ResponseWriter, r *Request) {
	pool, id, ok := r.poolFromPathAndID(w, c.lakeapi)
	if !ok {
		return
	}
	branchName, ok := r.StringFromPath(w, "branch")
	if !ok {
		return
	}
	var req api.IndexUpdateRequest
	if !r.Unmarshal(w, &req) {
		return
	}
	commit, err := c.lakeapi.UpdateIndex(r.Context(), req.Rules, pool, branchName)
	if err != nil {
		if errors.Is(err, commits.ErrEmptyTransaction) {
			err = srverr.ErrInvalid(err)
		}
		w.Error(err)
		return
	}
	w.Respond(http.StatusOK, api.CommitResponse{Commit: commit})
	c.publishEvent(w, "branch-commit", api.EventBranchCommit{
		CommitID: commit,
		PoolID:   id,
		Branch:   branchName,
	})
}

func handleVacuum(c *Core, w *ResponseWriter, r *Request) {
	pool, ok := r.StringFromPath(w, "pool")
	if !ok {
		return
	}
	revision, ok := r.StringFromPath(w, "revision")
	if !ok {
		return
	}
	dryrun, ok := r.BoolFromQuery(w, "dryrun")
	if !ok {
		return
	}
	lk := lakeapi.FromRoot(c.root)
	oids, err := lk.Vacuum(r.Context(), pool, revision, dryrun)
	if err != nil {
		w.Error(err)
		return
	}
	w.Respond(http.StatusOK, api.VacuumResponse{ObjectIDs: oids})
}

func handleVectorPost(c *Core, w *ResponseWriter, r *Request) {
	pool, ok := r.StringFromPath(w, "pool")
	if !ok {
		return
	}
	revision, ok := r.StringFromPath(w, "revision")
	if !ok {
		return
	}
	var req api.VectorRequest
	if !r.Unmarshal(w, &req) {
		return
	}
	message, ok := r.decodeCommitMessage(w)
	if !ok {
		return
	}
	lk := lakeapi.FromRoot(c.root)
	commit, err := lk.AddVectors(r.Context(), pool, revision, req.ObjectIDs, message)
	if err != nil {
		w.Error(err)
		return
	}
	w.Respond(http.StatusOK, api.CommitResponse{Commit: commit})
}

func handleVectorDelete(c *Core, w *ResponseWriter, r *Request) {
	pool, ok := r.StringFromPath(w, "pool")
	if !ok {
		return
	}
	revision, ok := r.StringFromPath(w, "revision")
	if !ok {
		return
	}
	var req api.VectorRequest
	if !r.Unmarshal(w, &req) {
		return
	}
	message, ok := r.decodeCommitMessage(w)
	if !ok {
		return
	}
	lk := lakeapi.FromRoot(c.root)
	commit, err := lk.DeleteVectors(r.Context(), pool, revision, req.ObjectIDs, message)
	if err != nil {
		w.Error(err)
		return
	}
	w.Respond(http.StatusOK, api.CommitResponse{Commit: commit})
}

func handleAuthIdentityGet(c *Core, w *ResponseWriter, r *Request) {
	ident := auth.IdentityFromContext(r.Context())
	w.Respond(http.StatusOK, api.AuthIdentityResponse{
		TenantID: string(ident.TenantID),
		UserID:   string(ident.UserID),
	})
}

func handleAuthMethodGet(c *Core, w *ResponseWriter, r *Request) {
	if c.auth == nil {
		w.Respond(http.StatusOK, api.AuthMethodResponse{Kind: api.AuthMethodNone})
		return
	}
	w.Respond(http.StatusOK, c.auth.MethodResponse())
}

func handleEvents(c *Core, w *ResponseWriter, r *Request) {
	format, err := api.MediaTypeToFormat(r.Header.Get("Accept"), "zson")
	if err != nil {
		w.Error(srverr.ErrInvalid(err))
	}
	writer := &eventStreamWriter{body: w.ResponseWriter, format: format}
	subscription := make(chan event)
	c.subscriptionsMu.Lock()
	c.subscriptions[subscription] = struct{}{}
	c.subscriptionsMu.Unlock()
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(200)
	// Flush header to notify clients that the request has been accepted.
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
	for {
		select {
		case ev := <-subscription:
			if err := writer.writeEvent(ev); err != nil {
				w.Error(err)
				continue
			}
			if f, ok := w.ResponseWriter.(http.Flusher); ok {
				f.Flush()
			}
		case <-r.Context().Done():
			c.subscriptionsMu.Lock()
			delete(c.subscriptions, subscription)
			c.subscriptionsMu.Unlock()
			return
		}
	}
}
