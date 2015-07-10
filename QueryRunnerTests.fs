module QueryTests

open NUnit.Framework
open Swensen.Unquote
open QueryRunner
open System.Threading

let slowExecutor input = 
    Thread.Sleep(100)
    Set[input]

[<Test>]
let ``simple query`` () =
    let query = SimpleQuery("obligation1")
    let result = runQuery slowExecutor query
    result =? Set["obligation1"]

[<Test>]
let ``complex query with OR aggregate`` () =
    let simpleQuery1 = SimpleQuery("obligation1")
    let simpleQuery2 = SimpleQuery("obligation2")

    let complexQuery = ComplexQuery(AggregateFunction.Union,[simpleQuery1;simpleQuery2])
    let result = runQuery slowExecutor complexQuery

    result =? Set["obligation1";"obligation2"]

[<Test>]
let ``complex query with AND aggregate`` () =
    let simpleQuery1 = SimpleQuery("obligation1")
    let simpleQuery2 = SimpleQuery("obligation2")

    let complexQuery = ComplexQuery(AggregateFunction.Intersect,[simpleQuery1;simpleQuery2])
    let result = runQuery slowExecutor complexQuery

    result =? Set[]  

[<Test>]
let ``complex query with Intersect and Union aggregate`` () =
    let simpleQuery1 = SimpleQuery("obligation1")
    let simpleQuery2 = SimpleQuery("obligation2")

    let complexQuery = ComplexQuery(AggregateFunction.Union,[simpleQuery1; simpleQuery1; simpleQuery2 ])
    let finalQuery = ComplexQuery(AggregateFunction.Intersect,[simpleQuery1;complexQuery])

    let result = runQuery slowExecutor finalQuery

    result =? Set["obligation1"] 
