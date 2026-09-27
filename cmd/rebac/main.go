// Command rebac provides local model and tuple diagnostics for the bundled KV store.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	rebac "github.com/surya-mp/go-rebac"
	"github.com/surya-mp/go-rebac/kv"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, out, errOut io.Writer) int {
	if len(args) == 0 {
		usage(errOut)
		return 2
	}
	var err error
	switch args[0] {
	case "model":
		err = runModel(args[1:], out)
	case "tuple":
		err = runTuple(args[1:], out)
	case "check", "expand", "explain":
		err = runDiagnostic(args, out)
	case "storage":
		err = runStorage(args[1:], out)
	default:
		usage(errOut)
		return 2
	}
	if err != nil {
		fmt.Fprintln(errOut, "rebac:", err)
		return 1
	}
	return 0
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "usage: rebac model validate|lint|publish|versions | tuple write|delete|read | check | expand | explain | storage check")
}

func runModel(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("model command required")
	}
	flags := flag.NewFlagSet("model "+args[0], flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	file, storePath, tenant, expected, id := "", "", "", "", ""
	flags.StringVar(&file, "file", "", "model JSON file")
	flags.StringVar(&storePath, "store", "", "KV database path")
	flags.StringVar(&tenant, "tenant", "", "tenant ID")
	flags.StringVar(&expected, "expected", "", "expected model version")
	flags.StringVar(&id, "id", "", "model ID")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	switch args[0] {
	case "validate", "lint":
		document, err := loadDocument(file)
		if err != nil {
			return err
		}
		if args[0] == "validate" {
			return encode(out, map[string]any{"valid": true, "id": document.ID})
		}
		return encode(out, rebac.LintModel(document.Model))
	case "publish":
		if tenant == "" || storePath == "" {
			return errors.New("--tenant and --store are required")
		}
		document, err := loadDocument(file)
		if err != nil {
			return err
		}
		store, err := openStore(storePath)
		if err != nil {
			return err
		}
		stored, err := store.WriteAuthorizationModel(context.Background(), tenant, document, rebac.Revision(expected))
		if err != nil {
			return err
		}
		return encode(out, stored)
	case "versions":
		if tenant == "" || id == "" || storePath == "" {
			return errors.New("--tenant, --id, and --store are required")
		}
		store, err := openStore(storePath)
		if err != nil {
			return err
		}
		versions, err := store.ListAuthorizationModelVersions(context.Background(), tenant, id)
		if err != nil {
			return err
		}
		return encode(out, versions)
	default:
		return errors.New("unknown model command")
	}
}

func runTuple(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("tuple command required")
	}
	flags := flag.NewFlagSet("tuple "+args[0], flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	storePath, modelPath, tuplePath, tenant, namespace, objectID, relation, user := "", "", "", "", "", "", "", ""
	flags.StringVar(&storePath, "store", "", "KV database path")
	flags.StringVar(&modelPath, "model", "", "model JSON file")
	flags.StringVar(&tuplePath, "tuple", "", "tuple JSON file")
	flags.StringVar(&tenant, "tenant", "", "tenant ID")
	flags.StringVar(&namespace, "namespace", "", "namespace")
	flags.StringVar(&objectID, "object", "", "object ID")
	flags.StringVar(&relation, "relation", "", "relation")
	flags.StringVar(&user, "user", "", "subject")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	engine, err := openEngine(storePath, modelPath)
	if err != nil {
		return err
	}
	switch args[0] {
	case "write", "delete":
		tuple, err := loadTuple(tuplePath)
		if err != nil {
			return err
		}
		var revision rebac.Revision
		if args[0] == "write" {
			revision, err = engine.WriteTupleWithRevision(context.Background(), tuple)
		} else {
			revision, err = engine.DeleteTupleWithRevision(context.Background(), tuple)
		}
		if err != nil {
			return err
		}
		return encode(out, map[string]rebac.Revision{"revision": revision})
	case "read":
		page, err := engine.ReadTuples(context.Background(), rebac.ReadTuplesRequest{TenantID: tenant, Filter: rebac.RelationTuple{Namespace: namespace, ObjectID: objectID, Relation: relation, User: user}})
		if err != nil {
			return err
		}
		return encode(out, page)
	default:
		return errors.New("unknown tuple command")
	}
}

func runDiagnostic(args []string, out io.Writer) error {
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	storePath, modelPath, tenant, user, relation, namespace, objectID := "", "", "", "", "", "", ""
	flags.StringVar(&storePath, "store", "", "KV database path")
	flags.StringVar(&modelPath, "model", "", "model JSON file")
	flags.StringVar(&tenant, "tenant", "", "tenant ID")
	flags.StringVar(&user, "user", "", "subject")
	flags.StringVar(&relation, "relation", "", "relation")
	flags.StringVar(&namespace, "namespace", "", "namespace")
	flags.StringVar(&objectID, "object", "", "object ID")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	engine, err := openEngine(storePath, modelPath)
	if err != nil {
		return err
	}
	switch args[0] {
	case "check":
		allowed, revision, err := engine.CheckWithRevision(context.Background(), "", tenant, user, relation, namespace, objectID)
		if err != nil {
			return err
		}
		return encode(out, map[string]any{"allowed": allowed, "revision": revision})
	case "expand":
		expansion, revision, err := engine.Expand(context.Background(), "", tenant, relation, namespace, objectID)
		if err != nil {
			return err
		}
		return encode(out, map[string]any{"expansion": expansion, "revision": revision})
	case "explain":
		explanation, err := engine.Explain(context.Background(), rebac.ExplainRequest{TenantID: tenant, User: user, Relation: relation, Namespace: namespace, ObjectID: objectID})
		if err != nil {
			return err
		}
		return encode(out, explanation)
	}
	return errors.New("unknown diagnostic command")
}

func runStorage(args []string, out io.Writer) error {
	if len(args) == 0 || args[0] != "check" {
		return errors.New("usage: rebac storage check --store PATH")
	}
	flags := flag.NewFlagSet("storage check", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	storePath := ""
	flags.StringVar(&storePath, "store", "", "KV database path")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if _, err := openStore(storePath); err != nil {
		return err
	}
	return encode(out, map[string]bool{"storage_open": true, "revisioned": true, "mutations": true, "candidate_readers": true, "watches": true})
}

func openEngine(storePath, modelPath string) (*rebac.Engine, error) {
	if storePath == "" || modelPath == "" {
		return nil, errors.New("--store and --model are required")
	}
	document, err := loadDocument(modelPath)
	if err != nil {
		return nil, err
	}
	model, err := document.Compile(nil)
	if err != nil {
		return nil, err
	}
	store, err := openStore(storePath)
	if err != nil {
		return nil, err
	}
	return rebac.NewEngine(store, model)
}

func openStore(path string) (*kv.ReBACStore, error) {
	if path == "" {
		return nil, errors.New("--store is required")
	}
	database, err := kv.Open(path)
	if err != nil {
		return nil, err
	}
	return kv.NewReBACStore(database), nil
}

func loadDocument(path string) (rebac.ModelDocument, error) {
	if path == "" {
		return rebac.ModelDocument{}, errors.New("--file or --model is required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return rebac.ModelDocument{}, err
	}
	var document rebac.ModelDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		return rebac.ModelDocument{}, err
	}
	if document.Model.Namespaces == nil {
		if err := json.Unmarshal(raw, &document.Model); err != nil {
			return rebac.ModelDocument{}, err
		}
		document.ID = "local"
	}
	if err := document.Validate(); err != nil {
		return rebac.ModelDocument{}, err
	}
	return document, nil
}

func loadTuple(path string) (rebac.RelationTuple, error) {
	if path == "" {
		return rebac.RelationTuple{}, errors.New("--tuple is required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return rebac.RelationTuple{}, err
	}
	var tuple rebac.RelationTuple
	return tuple, json.Unmarshal(raw, &tuple)
}

func encode(w io.Writer, value any) error { return json.NewEncoder(w).Encode(value) }
