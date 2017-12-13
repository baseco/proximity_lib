-module(proximity_lib_pg).

-include_lib("epgsql/include/epgsql.hrl").

%% API exports
-export([start/0]).
-export([squery/1]).
-export([equery/2]).
-export([squery_read/1]).
-export([equery_read/2]).

-define(PG_MASTER_POOL, 'PG_MASTER_POOL').
-define(PG_GROUP_REPLICA, replica).
-define(PG_GROUP_MASTER, master).
-define(PG_POOL_SIZE, 15).
-define(PG_POOL_SIZE_MAX, 50).

%%====================================================================
%% API functions
%%====================================================================

squery_read(Query) ->
    {Type, Pid} = get_pid(?PG_GROUP_REPLICA),
    try
        Ret = epgsql:squery(Pid, Query),
        parse(Ret)
    after
        close_or_return_pid(Type, ?PG_GROUP_REPLICA, Pid)
    end.

equery_read(Query, Params) ->
    {Type, Pid} = get_pid(?PG_GROUP_REPLICA),
    try
        Ret = epgsql:equery(Pid, Query, Params),
        parse(Ret)
    after
        close_or_return_pid(Type, ?PG_GROUP_REPLICA, Pid)
    end.

squery(Query) ->
    {Type, Pid} = get_pid(?PG_GROUP_MASTER),
    try
        Ret = epgsql:squery(Pid, Query),
        parse(Ret)
    after
        close_or_return_pid(Type, ?PG_GROUP_MASTER, Pid)
    end.

equery(Query, Params) ->
    {Type, Pid} = get_pid(?PG_GROUP_MASTER),
    try
        Ret = epgsql:equery(Pid, Query, Params),
        parse(Ret)
    after
        close_or_return_pid(Type, ?PG_GROUP_MASTER, Pid)
    end.

start() ->
    start_pools().

%%====================================================================
%% Internal functions
%%====================================================================

start_pools() ->
    case os:getenv("PG_POOLS") of
        false ->
            ok;
        Pools ->
            PoolsBin = list_to_binary(Pools),
            PoolsList = binary:split(PoolsBin, <<":">>, [global]),
            [begin
                EnvKey = binary_to_list(<<"PG_", Pool/binary>>),
                case os:getenv(EnvKey) of
                    false ->
                        ok;
                    PoolConfig ->
                        {PgUser, PgPassword, PgHost, PgDatabase} = parse_pool_config(list_to_binary(PoolConfig)),
                        PgOpts = [{database, PgDatabase}],
                        Group = case Pool of
                            <<"MASTER">> ->
                                master;
                            _  ->
                                replica
                        end,
                        Args = [PgHost, PgUser, PgPassword, PgOpts],
                        ok = store_conn_fun(Group, Args),
                        Config = [
                            {name, pg_pool_name(Pool)},
                            {group, Group},
                            {max_count, pool_size_max()},
                            {init_count, pool_size()},
                            {start_mfa, {epgsql, connect, Args}}
                        ],
                        {ok, _Pid} = pooler:new_pool(Config),
                        ok
                end
            end || Pool <- PoolsList]
    end.

get_pid(Group) ->
    case pooler:take_group_member(Group) of
        Pid when is_pid(Pid) ->
            {pool, Pid};
        error_no_members ->
            lager:warning("Starting new direct connection for group ~p", [Group]),
            {ok, List} = application:get_env(proximity_lib, pg_replica_fun),
            Fun = pick_random(List),
            {ok, Pid} = Fun(),
            {direct, Pid}
    end.

close_or_return_pid(pool, Group, Pid) ->
    pooler:return_group_member(Group, Pid, ok);
close_or_return_pid(direct, _Group, Pid) ->
    catch epgsql:close(Pid).

store_conn_fun(master, Args) ->
    application:set_env(proximity_lib, pg_master_fun, fun() -> apply(epgsql, connect, Args) end);
store_conn_fun(replica, Args) ->
    List = application:get_env(proximity_lib, pg_replica_fun, []),
    application:set_env(proximity_lib, pg_replica_fun, [fun() -> apply(epgsql, connect, Args) end | List]).

pick_random(List) ->
    Index = erlang:phash(os:timestamp(), length(List)),
    lists:nth(Index, List).

pool_size() ->
    case os:getenv("PG_POOL_SIZE") of
        false ->
            ?PG_POOL_SIZE;
        Size ->
            list_to_integer(Size)
    end.

pool_size_max() ->
    case os:getenv("PG_POOL_SIZE_MAX") of
        false ->
            ?PG_POOL_SIZE_MAX;
        Size ->
            list_to_integer(Size)
    end.

%% Example: user:password@host/database
parse_pool_config(PoolConfig) ->
    [UserPassword, Rest] = binary:split(PoolConfig, <<"@">>),
    [PgUser, PgPassword] = binary:split(UserPassword, <<":">>),
    [PgHost, PgDatabase] = binary:split(Rest, <<"/">>),
    {binary_to_list(PgUser), binary_to_list(PgPassword), binary_to_list(PgHost), binary_to_list(PgDatabase)}.

pg_pool_name(<<"MASTER">>) ->
    ?PG_MASTER_POOL;
pg_pool_name(Pool) ->
    binary_to_atom(<<"PG_", Pool/binary, "_">>, utf8).

parse({error, #error{code = <<"23505">>, extra = Extra}}) ->
    {match, [Column]} = re:run(proplists:get_value(detail, Extra), "Key \\(([^\\)]+)\\)", [{capture, all_but_first, binary}]),
    throw({error, {non_unique, Column}});
parse({error, #error{code = <<"23503">>}}) ->
    throw({error, fkey_not_found});
parse({error, #error{message = Msg} = Error}) ->
    lager:error("PostgreSQL error ~p", [Error]),
    throw({error, Msg});
parse({ok, Cols, Rows}) ->
    {ok, to_map(Cols, Rows)};
parse({ok, Counts, Cols, Rows}) ->
    {ok, Counts, to_map(Cols, Rows)};
parse(Result) ->
    Result.

to_map(Cols, Rows) ->
    [maps:from_list(lists:zipwith(fun(#column{name = N}, V) -> {N, V} end, Cols, tuple_to_list(Row))) || Row <- Rows].
