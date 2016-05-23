// Learn more about F# at http://fsharp.org

open System
open System.Threading.Tasks
open Npgsql
open NpgsqlTypes
open org.apache.zookeeper
open org.apache.utils
open Microsoft.AspNetCore.Hosting
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Http
open Microsoft.Extensions.Logging
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Logging.Console

type WaitToConnect() =
    inherit Watcher()
    let connectedCompletionSource = TaskCompletionSource()
    member cw.WaitAsync () = connectedCompletionSource.Task
    override cw.``process``(evt:WatchedEvent) =
        match evt.getState() with
        | Watcher.Event.KeeperState.SyncConnected -> connectedCompletionSource.TrySetResult(true) |> ignore
        | _ -> ()
        Task.FromResult(true) :> Task

type Startup() = 
    member x.Configure (app:IApplicationBuilder, hosting:IHostingEnvironment, loggerFactory:ILoggerFactory) =
        loggerFactory.AddConsole(LogLevel.Debug) |> ignore
        let logger = loggerFactory.CreateLogger("MyApp")
        let reqHandler (ctx: HttpContext) = 
            async {
                sprintf "Received request at %O" ctx.Request.Path
                |> logger.LogInformation
                ctx.Response.ContentType <- "application/json"
                do! ctx.Response.WriteAsync ("""{"hello":"world"}""") |> Async.AwaitTask
            } |> Async.StartAsTask :> Task
        app.Run(RequestDelegate(reqHandler))
      
[<EntryPoint>]
let main argv = 
    async {
        let username = Environment.GetEnvironmentVariable("USER")
        let connStr = sprintf "Server=127.0.0.1;Port=5432;Database=policy;User Id=%s;Pooling=false" username
        use conn = new NpgsqlConnection(connStr)
        conn.Open()
        printfn "Connected to database at %s:%i" conn.Host conn.Port
        use cmd = conn.CreateCommand()
        cmd.CommandText <- "select * from information_schema.tables where table_schema = 'public'"
        use rdr = cmd.ExecuteReader()
        let rec read (r:System.Data.IDataReader) = 
            match r.Read() with
            | true -> printfn "%O | %O | %O" r.[0] r.[1] r.[2]; read r
            | false -> ()
        rdr |> read
        let zkUrl = "ubuntu-vfi.local:2181"
        let sessionTimeout = 10000
        let watcher = WaitToConnect ()
        let zk = ZooKeeper(zkUrl, sessionTimeout, watcher)
        let! connected = watcher.WaitAsync () |> Async.AwaitTask
        printfn "Connected to ZK at %s." zkUrl
        do! zk.closeAsync() |> Async.AwaitTask
        printfn "Disconnected from ZK at %s." zkUrl
    } |> Async.RunSynchronously
    let h = WebHostBuilder().UseKestrel().UseStartup<Startup>().Build()
    h.Run()
    0
