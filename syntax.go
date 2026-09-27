package rebac

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

const maxNameLength = 63

// Names in an authorization model use lower_snake_case. Keeping names
// canonical makes tuples portable across storage implementations.
func validName(value string) bool {
	if len(value) == 0 || len(value) > maxNameLength {
		return false
	}
	for i := range value {
		c := value[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
		if i == 0 && (c < 'a' || c > 'z') {
			return false
		}
	}
	return true
}

// Object IDs are application-owned opaque strings. Separators and whitespace
// are reserved so a subject always has one unambiguous wire representation.
func validObjectID(value string) bool {
	if value == "" || !utf8.ValidString(value) || strings.Contains(value, "#") {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return false
		}
	}
	return true
}

func validateSubjectSyntax(subject string) error {
	resource, relation, userset := strings.Cut(subject, "#")
	if userset && (!validName(relation) || strings.Contains(relation, "#")) {
		return fmt.Errorf("%w: malformed userset %q", ErrInvalidTuple, subject)
	}
	namespace, objectID, ok := strings.Cut(resource, ":")
	if !ok || !validName(namespace) || !validObjectID(objectID) {
		return fmt.Errorf("%w: subjects must be namespace:objectID or namespace:objectID#relation", ErrInvalidTuple)
	}
	return nil
}

// ValidateSyntax checks the portable Zanzibar-style tuple grammar before a
// datastore sees the tuple. Model-specific checks happen in Engine writes.
func (t RelationTuple) ValidateSyntax() error {
	if !validObjectID(t.TenantID) {
		return fmt.Errorf("%w: invalid tenant ID", ErrInvalidTuple)
	}
	if !validName(t.Namespace) || !validObjectID(t.ObjectID) || !validName(t.Relation) {
		return fmt.Errorf("%w: invalid resource %q:%q#%q", ErrInvalidTuple, t.Namespace, t.ObjectID, t.Relation)
	}
	if err := validateSubjectSyntax(t.User); err != nil {
		return err
	}
	if t.Caveat == "" && len(t.CaveatContext) != 0 {
		return fmt.Errorf("%w: caveat context without a caveat", ErrInvalidTuple)
	}
	if t.Caveat != "" && !validName(t.Caveat) {
		return fmt.Errorf("%w: invalid caveat %q", ErrInvalidTuple, t.Caveat)
	}
	if t.NotBeforeUnixNano != 0 && t.NotAfterUnixNano != 0 && t.NotBeforeUnixNano >= t.NotAfterUnixNano {
		return fmt.Errorf("%w: invalid validity interval", ErrInvalidTuple)
	}
	return nil
}
