-module(proximity_lib_pg).

-include_lib("epgsql/include/epgsql.hrl").

%% API exports
-export([start/0]).
-export([squery/1]).
-export([equery/2]).

-define(PG_POOL_NAME, proximity_lib_pg_pool).
-define(PG_POOL_SIZE, 15).

%%====================================================================
%% API functions
%%====================================================================

squery(Query) ->
    Ret = epgsql:squery(cuesport:get_worker(?PG_POOL_NAME), Query),
    parse(Ret).

equery(Query, Params) ->
    Ret = epgsql:equery(cuesport:get_worker(?PG_POOL_NAME), Query, Params),
    parse(Ret).

start() ->
    case os:getenv("PG_HOST") of
        false ->
            ok;
        PgHost ->
            ChildMods = [epgsql],
            ChildMF = {epgsql, connect},
            PgUser = os:getenv("PG_USER"),
            PgDatabase = os:getenv("PG_DATABASE"),
            PgPassword = os:getenv("PG_PASSWORD"),
            PgOpts = [{database, PgDatabase}],
            ChildArgs = [PgHost, PgUser, PgPassword, PgOpts],
            _ = cuesport:start_link(?PG_POOL_NAME, ?PG_POOL_SIZE, ChildMods, ChildMF, {for_all, ChildArgs}),
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
