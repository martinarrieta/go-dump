package main

import "fmt"
import "reflect"

func main() {
  var s map[int]interface{}
  if _, ok := s[1].(*string); ok {
    /* act on str */
  } else {
      /* not string */
  }

  fmt.Println("s :", s, reflect.TypeOf(s[1]))
}
