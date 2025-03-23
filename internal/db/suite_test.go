package db_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDatabaseSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Database Tests Suite")
}
