module QueryRunner
    type AggregateFunction = Intersect | Union
    type Query = 
        | SimpleQuery of string
        | ComplexQuery of AggregateFunction * Query list

    let aggregateResults aggregate =
            match aggregate with
            | Intersect -> Set.intersectMany
            | Union -> Set.unionMany

    let rec runQuery executor query = 
        match query with 
        | SimpleQuery q -> executor q
        | ComplexQuery (aggregateFunction,queries) -> 
                                            queries
                                                |> Seq.map (fun q -> runQuery executor q)
                                                |> Set.ofSeq
                                                |> aggregateResults aggregateFunction
    
    #nowarn "40"

    type countMsg =
    | Die
    | Incr of int
    | Fetch of AsyncReplyChannel<int>

    let printerAgent = MailboxProcessor.Start(fun inbox-> 
        // the message processing function
        let rec messageLoop = async{
            // read a message
            let! msg = inbox.Receive()
        
            // process a message
            match msg with
            | Die -> return ()
            | Incr x -> return! messageLoop
            | Fetch(reply) ->
                reply.Reply(1);
                return! messageLoop

            // loop to top
            return! messageLoop  
            }

        // start the loop 
        messageLoop 
    )

    let compute = 
        printerAgent.PostAndAsyncReply (fun reply -> Fetch(reply))
        |> Async.RunSynchronously
