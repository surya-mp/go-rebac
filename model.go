package rebac

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrInvalidModel = errors.New("rebac: invalid authorization model")
	ErrInvalidTuple = errors.New("rebac: invalid relation tuple")
)

// AuthorizationModel declares the object types and relationships an
// application permits. Pass it to NewEngine at application startup.
type AuthorizationModel struct {
	Namespaces map[string]NamespaceDefinition `json:"namespaces"`
	Caveats    map[string]CaveatDefinition    `json:"caveats,omitempty"`
}

// CaveatContext carries tuple parameters and request values into an
// application-defined, deterministic caveat evaluator.
type CaveatContext map[string]any

// CaveatEvaluator must be deterministic and must not perform I/O. Returning
// false denies only the tuple carrying the caveat.
type CaveatEvaluator func(context.Context, CaveatContext, CaveatContext) (bool, error)

// CaveatDefinition is owned by the application alongside its model.
type CaveatDefinition struct {
	Evaluate CaveatEvaluator `json:"-"`
}

// NamespaceDefinition declares relations available on one object type. A
// namespace such as "user" may have no relations and only represent subjects.
type NamespaceDefinition struct {
	Relations map[string]RelationDefinition `json:"relations"`
}

// RelationDefinition lists the subject types allowed in a relation.
type RelationDefinition struct {
	AllowedSubjects []SubjectReference `json:"allowed_subjects,omitempty"`
	AllowWildcard   bool               `json:"allow_wildcard,omitempty"`
	Rewrite         Rewrite            `json:"rewrite,omitempty"`
}

// SubjectReference names a direct subject type (Relation empty), or a userset
// such as group#member.
type SubjectReference struct {
	Namespace string `json:"namespace"`
	Relation  string `json:"relation,omitempty"`
}

// Rewrite defines how a relation is derived. A zero Rewrite preserves the
// original direct-tuple behavior (This).
type Rewrite struct {
	This            bool            `json:"this,omitempty"`
	ComputedUserset string          `json:"computed_userset,omitempty"`
	TupleToUserset  *TupleToUserset `json:"tuple_to_userset,omitempty"`
	Union           []Rewrite       `json:"union,omitempty"`
	Intersection    []Rewrite       `json:"intersection,omitempty"`
	Exclusion       *Exclusion      `json:"exclusion,omitempty"`
}

// TupleToUserset follows Tupleset on the current object, then evaluates
// ComputedUserset on each referenced object.
type TupleToUserset struct {
	Tupleset        string `json:"tupleset"`
	ComputedUserset string `json:"computed_userset"`
}

// Exclusion grants Base unless Subtract also grants access.
type Exclusion struct {
	Base     Rewrite `json:"base"`
	Subtract Rewrite `json:"subtract"`
}

// Validate checks model structure and its canonical namespace/relation names.
func (m AuthorizationModel) Validate() error { return m.validate() }

func (m AuthorizationModel) validate() error {
	if len(m.Namespaces) == 0 {
		return fmt.Errorf("%w: no namespaces", ErrInvalidModel)
	}
	for namespace, definition := range m.Namespaces {
		if !validName(namespace) {
			return fmt.Errorf("%w: invalid namespace %q", ErrInvalidModel, namespace)
		}
		for relation, definition := range definition.Relations {
			if !validName(relation) {
				return fmt.Errorf("%w: invalid relation %q in %q", ErrInvalidModel, relation, namespace)
			}
			for _, subject := range definition.AllowedSubjects {
				if _, ok := m.Namespaces[subject.Namespace]; !validName(subject.Namespace) || !ok {
					return fmt.Errorf("%w: unknown subject namespace %q", ErrInvalidModel, subject.Namespace)
				}
				if subject.Relation != "" {
					if _, ok := m.Namespaces[subject.Namespace].Relations[subject.Relation]; !ok {
						return fmt.Errorf("%w: unknown userset %s#%s", ErrInvalidModel, subject.Namespace, subject.Relation)
					}
				}
			}
			if err := m.validateRewrite(namespace, definition.Rewrite); err != nil {
				return err
			}
		}
	}
	for name := range m.Caveats {
		if !validName(name) {
			return fmt.Errorf("%w: invalid caveat %q", ErrInvalidModel, name)
		}
	}
	return nil
}

func (m AuthorizationModel) validateRewrite(namespace string, rewrite Rewrite) error {
	kind, err := rewrite.kind()
	if err != nil {
		return err
	}
	switch kind {
	case "this":
		return nil
	case "computed":
		if _, ok := m.Namespaces[namespace].Relations[rewrite.ComputedUserset]; !ok {
			return fmt.Errorf("%w: unknown computed userset %s#%s", ErrInvalidModel, namespace, rewrite.ComputedUserset)
		}
	case "tuple-to-userset":
		if err := m.validateTupleToUserset(namespace, *rewrite.TupleToUserset); err != nil {
			return err
		}
	case "union", "intersection":
		children := rewrite.Union
		if kind == "intersection" {
			children = rewrite.Intersection
		}
		for _, child := range children {
			if err := m.validateRewrite(namespace, child); err != nil {
				return err
			}
		}
	case "exclusion":
		if err := m.validateRewrite(namespace, rewrite.Exclusion.Base); err != nil {
			return err
		}
		if err := m.validateRewrite(namespace, rewrite.Exclusion.Subtract); err != nil {
			return err
		}
	}
	return nil
}

func (m AuthorizationModel) validateTupleToUserset(namespace string, rewrite TupleToUserset) error {
	definition, ok := m.Namespaces[namespace].Relations[rewrite.Tupleset]
	if rewrite.Tupleset == "" || !ok {
		return fmt.Errorf("%w: unknown tupleset %s#%s", ErrInvalidModel, namespace, rewrite.Tupleset)
	}
	if rewrite.ComputedUserset == "" {
		return fmt.Errorf("%w: empty tuple-to-userset relation", ErrInvalidModel)
	}
	found := false
	for _, subject := range definition.AllowedSubjects {
		if subject.Relation != "" {
			continue
		}
		if _, ok := m.Namespaces[subject.Namespace].Relations[rewrite.ComputedUserset]; !ok {
			return fmt.Errorf("%w: unknown computed userset %s#%s", ErrInvalidModel, subject.Namespace, rewrite.ComputedUserset)
		}
		found = true
	}
	if !found {
		return fmt.Errorf("%w: tupleset %s#%s has no direct object subjects", ErrInvalidModel, namespace, rewrite.Tupleset)
	}
	return nil
}

func (r Rewrite) kind() (string, error) {
	count := 0
	if r.This {
		count++
	}
	if r.ComputedUserset != "" {
		count++
	}
	if r.TupleToUserset != nil {
		count++
	}
	if len(r.Union) != 0 {
		count++
	}
	if len(r.Intersection) != 0 {
		count++
	}
	if r.Exclusion != nil {
		count++
	}
	if count == 0 {
		return "this", nil
	}
	if count != 1 {
		return "", fmt.Errorf("%w: rewrite must have exactly one operation", ErrInvalidModel)
	}
	switch {
	case r.This:
		return "this", nil
	case r.ComputedUserset != "":
		return "computed", nil
	case r.TupleToUserset != nil:
		return "tuple-to-userset", nil
	case len(r.Union) != 0:
		return "union", nil
	case len(r.Intersection) != 0:
		return "intersection", nil
	default:
		return "exclusion", nil
	}
}

func (m AuthorizationModel) validateTuple(tuple RelationTuple) error {
	if err := tuple.ValidateSyntax(); err != nil {
		return err
	}
	definition, ok := m.Namespaces[tuple.Namespace]
	if tuple.Namespace == "" || !ok {
		return fmt.Errorf("%w: unknown namespace %q", ErrInvalidTuple, tuple.Namespace)
	}
	relation, ok := definition.Relations[tuple.Relation]
	if tuple.ObjectID == "" || tuple.Relation == "" || !ok {
		return fmt.Errorf("%w: unknown or empty relation %q", ErrInvalidTuple, tuple.Relation)
	}
	if tuple.User == "user:*" {
		if !relation.AllowWildcard {
			return fmt.Errorf("%w: wildcard is not allowed on %s#%s", ErrInvalidTuple, tuple.Namespace, tuple.Relation)
		}
		return m.validateCaveat(tuple)
	}

	subject, err := m.subjectReference(tuple.User)
	if err != nil {
		return err
	}
	for _, allowed := range relation.AllowedSubjects {
		if allowed == subject {
			return m.validateCaveat(tuple)
		}
	}
	return fmt.Errorf("%w: %s cannot be assigned to %s#%s", ErrInvalidTuple, tuple.User, tuple.Namespace, tuple.Relation)
}

func (m AuthorizationModel) validateCaveat(tuple RelationTuple) error {
	if tuple.Caveat == "" {
		return nil
	}
	definition, ok := m.Caveats[tuple.Caveat]
	if !ok || definition.Evaluate == nil {
		return fmt.Errorf("%w: unknown caveat %q", ErrInvalidTuple, tuple.Caveat)
	}
	return nil
}

func (m AuthorizationModel) validateCheck(user, relation, namespace, objectID string) error {
	if !validObjectID(objectID) {
		return fmt.Errorf("%w: empty object ID", ErrInvalidTuple)
	}
	if _, ok := m.Namespaces[namespace]; !validName(namespace) || !ok {
		return fmt.Errorf("%w: unknown namespace %q", ErrInvalidTuple, namespace)
	}
	if _, ok := m.Namespaces[namespace].Relations[relation]; !validName(relation) || !ok {
		return fmt.Errorf("%w: unknown relation %q", ErrInvalidTuple, relation)
	}
	if user == "user:*" {
		return fmt.Errorf("%w: wildcard is not a valid check subject", ErrInvalidTuple)
	}
	_, err := m.subjectReference(user)
	return err
}

func (m AuthorizationModel) subjectReference(user string) (SubjectReference, error) {
	if err := validateSubjectSyntax(user); err != nil {
		return SubjectReference{}, err
	}
	if strings.Contains(user, "#") {
		namespace, _, relation, ok := parseUserset(user)
		if !ok {
			return SubjectReference{}, fmt.Errorf("%w: malformed userset %q", ErrInvalidTuple, user)
		}
		if _, ok := m.Namespaces[namespace]; !ok {
			return SubjectReference{}, fmt.Errorf("%w: unknown subject namespace %q", ErrInvalidTuple, namespace)
		}
		if _, ok := m.Namespaces[namespace].Relations[relation]; !ok {
			return SubjectReference{}, fmt.Errorf("%w: unknown userset %s#%s", ErrInvalidTuple, namespace, relation)
		}
		return SubjectReference{Namespace: namespace, Relation: relation}, nil
	}
	namespace, objectID, ok := strings.Cut(user, ":")
	if !ok || namespace == "" || objectID == "" {
		return SubjectReference{}, fmt.Errorf("%w: direct subjects must be namespace:objectID", ErrInvalidTuple)
	}
	if _, ok := m.Namespaces[namespace]; !ok {
		return SubjectReference{}, fmt.Errorf("%w: unknown subject namespace %q", ErrInvalidTuple, namespace)
	}
	return SubjectReference{Namespace: namespace}, nil
}
