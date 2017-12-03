-module(proximity_lib_pg).

-include_lib("epgsql/include/epgsql.hrl").

%% API exports
-export([start/0]).
-export([squery/1]).
-export([equery/2]).
-export([squery_read/1]).
-export([equery_read/2]).

-define(PG_MASTER_POOL, 'PG_MASTER_POOL').
-define(PG_GROUP, pg).
-define(PG_POOL_SIZE, 15).
-define(PG_POOL_SIZE_MAX, 50).

%%====================================================================
%% API functions
%%====================================================================

squery_read(Query) ->
    Pid = pooler:take_group_member(?PG_GROUP),
    try
        Ret = epgsql:squery(Pid, Query),
        parse(Ret)
    after
        pooler:return_group_member(?PG_GROUP, Pid, ok)
    end.

equery_read(Query, Params) ->
    Pid = pooler:take_group_member(?PG_GROUP),
    try
        Ret = epgsql:equery(Pid, Query, Params),
        parse(Ret)
    after
        pooler:return_group_member(?PG_GROUP, Pid, ok)
    end.

squery(Query) ->
    Pid = pooler:take_member(?PG_MASTER_POOL),
    try
        Ret = epgsql:squery(Pid, Query),
        parse(Ret)
    after
        pooler:return_member(?PG_MASTER_POOL, Pid, ok)
    end.

equery(Query, Params) ->
    Pid = pooler:take_member(?PG_MASTER_POOL),
    try
        Ret = epgsql:equery(Pid, Query, Params),
        parse(Ret)
    after
        pooler:return_member(?PG_MASTER_POOL, Pid, ok)
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
                        [PgUser, PgDatabase, PgPassword, PgHost, PgPoolSize, PgPoolSizeMax] = parse_pool_config(list_to_binary(PoolConfig)),
                        PgOpts = [{database, PgDatabase}],
                        Config = [
                            {name, pg_pool_name(Pool)},
                            {group, pg},
                            {max_count, PgPoolSizeMax},
                            {init_count, PgPoolSize},
                            {start_mfa, {epgsql, connect, [PgHost, PgUser, PgPassword, PgOpts]}}
                        ],
                        {ok, _Pid} = pooler:new_pool(Config),
                        ok
                end
            end || Pool <- PoolsList]
    end.

%% Example: proximity::proximity::password::host::15::50
parse_pool_config(PoolConfig) ->
    case binary:split(PoolConfig, <<"::">>, [global]) of
        [PgUser, PgDatabase, PgPassword, PgHost, PgPoolSize, PgPoolSizeMax] ->
            [binary_to_list(PgUser), binary_to_list(PgDatabase), binary_to_list(PgPassword), binary_to_list(PgHost),
                binary_to_integer(PgPoolSize), binary_to_integer(PgPoolSizeMax)];
        [PgUser, PgDatabase, PgPassword, PgHost] ->
            [binary_to_list(PgUser), binary_to_list(PgDatabase), binary_to_list(PgPassword), binary_to_list(PgHost), ?PG_POOL_SIZE, ?PG_POOL_SIZE_MAX]
    end.

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
