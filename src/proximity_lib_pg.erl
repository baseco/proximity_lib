-module(proximity_lib_pg).

-include_lib("epgsql/include/epgsql.hrl").

%% API exports
-export([start/0]).
-export([squery/1]).
-export([equery/2]).

-define(PG_POOL_NAME, proximity_lib_pg_pool).
-define(PG_POOL_SIZE, 15).
-define(PG_POOL_SIZE_MAX, 50).

%%====================================================================
%% API functions
%%====================================================================

squery(Query) ->
    Pid = pooler:take_member(?PG_POOL_NAME),
    try
        Ret = epgsql:squery(Pid, Query),
        parse(Ret)
    after
        pooler:return_member(?PG_POOL_NAME, Pid, ok)
    end.

equery(Query, Params) ->
    Pid = pooler:take_member(?PG_POOL_NAME),
    try
        Ret = epgsql:equery(Pid, Query, Params),
        parse(Ret)
    after
        pooler:return_member(?PG_POOL_NAME, Pid, ok)
    end.

start() ->
    case os:getenv("PG_HOST") of
        false ->
            ok;
        PgHost ->
            PgUser = os:getenv("PG_USER"),
            PgDatabase = os:getenv("PG_DATABASE"),
            PgPassword = os:getenv("PG_PASSWORD"),
            PgPoolSize = os:getenv("PG_POOL_SIZE"),
            PgPoolSizeMax = os:getenv("PG_POOL_SIZE_MAX"),
            PgOpts = [{database, PgDatabase}],
            PoolConfig = [
                {name, ?PG_POOL_NAME},
                {group, pg},
                {max_count, PgPoolSizeMax},
                {init_count, PgPoolSize},
                {start_mfa, {epgsql, connect, [PgHost, PgUser, PgPassword, PgOpts]}}
            ],
            _ = pooler:new_pool(PoolConfig),
            ok
    end.

%%====================================================================
%% Internal functions
%%====================================================================

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
