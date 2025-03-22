package main

type A struct {
	a int
}

type B struct {
	A
	b int
}

/*
func main() {
	obj := B{
		A: A{
			a: 1,
		},
		b: 2,
	}

	fmt.Println(obj.a)
}
*/
