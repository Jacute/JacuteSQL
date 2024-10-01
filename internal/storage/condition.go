package storage

import (
	"JacuteSQL/internal/data_structures/mymap"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// Тип узла
type NodeType int

const (
	ConditionNode NodeType = iota
	OrNode
	AndNode
)

// Структура для узла дерева выражений
type Node struct {
	NodeType    NodeType
	Value       string
	Left, Right *Node
}

// GetConditionTree splits the condition into tree with priority
func (s *Storage) GetConditionTree(query string) *Node {
	orParts := splitByOperator(query, "OR")

	// OR
	if len(orParts) > 1 {
		root := &Node{NodeType: OrNode}
		root.Left = s.GetConditionTree(orParts[0])
		root.Right = s.GetConditionTree(strings.Join(orParts[1:], "OR"))
		return root
	}

	// AND
	andParts := splitByOperator(query, "AND")
	if len(andParts) > 1 {
		root := &Node{NodeType: AndNode}
		root.Left = s.GetConditionTree(andParts[0])
		root.Right = s.GetConditionTree(strings.Join(andParts[1:], "AND"))
		return root
	}

	// For simple condition
	return &Node{NodeType: ConditionNode, Value: strings.TrimSpace(query)}
}

func splitByOperator(query, operator string) []string {
	operatorPattern := fmt.Sprintf(`\s+%s\s+`, operator)
	re := regexp.MustCompile(operatorPattern)
	return re.Split(query, -1)
}

// IsValidRow checks row by tree with conditions
//
// neededTables - all tables for condition, curTable - current table for condition
func (s *Storage) IsValidRow(node *Node, row *mymap.CustomMap, neededTables []string, curTable string) bool {
	if node == nil {
		return false
	}
	switch node.NodeType {
	case ConditionNode:
		parts := strings.SplitN(node.Value, "=", 2)
		if len(parts) < 2 {
			return false
		}
		part1, value := strings.TrimSpace(parts[0]), strings.Trim(strings.TrimSpace(parts[1]), "'")
		part1Splitted := strings.SplitN(part1, ".", 2)
		if len(part1Splitted) != 2 {
			return false
		}
		table, _ := part1Splitted[0], part1Splitted[1]
		if !slices.Contains(neededTables, table) {
			return false
		}
		rowValue, _ := row.Get(part1).(string)
		if curTable == table && rowValue != value {
			return false
		}
		return true
	case OrNode:
		return s.IsValidRow(node.Left, row, neededTables, curTable) || s.IsValidRow(node.Right, row, neededTables, curTable)
	case AndNode:
		return s.IsValidRow(node.Left, row, neededTables, curTable) && s.IsValidRow(node.Right, row, neededTables, curTable)
	default:
		return false
	}
}
