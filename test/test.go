package main

import (
	"go/ast"
	"go/parser"
	"go/token"
)

func main() {
	src := `package main

		import "fmt"
		
		func main() {
			var a int = 1
		
			for i:=0;i<10;i++ {
				a = a + 1
				fmt.Println("hello world!")
			}
		}
		`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "", src, 0)
	if err != nil {
		panic(err)
	}
	ast.Print(fset, f)
}
