set lipstick.server.url 'http://localhost:8080/lipstick';

--
-- Aims to be a more complex example to fully exercise
-- lipstick features.
--

--
-- Find triangles in a graph.
--
-- See: http://www.computer.org/portal/web/csdl/doi/10.1109/MCSE.2009.120 for full discussion
--

--
-- First, load the graph and duplicate it logically
--
graph = load 'graph.tsv' as (v1:chararray, v2:chararray);

graph_dup = foreach graph generate v1, v2;

--
-- With a single map-reduce job, compute the degrees
-- for each vertex while preserving the edges
--
graph_cog = cogroup graph by v1 outer, graph_dup by v2;

v1_degrees = foreach graph_cog generate
               flatten(graph) as (v1, v2),
               COUNT(graph) + COUNT(graph_dup) as deg_v1;    

v2_degrees = foreach graph_cog generate
               flatten(graph_dup) as (v1, v2),
               COUNT(graph) + COUNT(graph_dup) as deg_v2;

--
-- Next filter to keep edges having nodes with degree > 1
-- since nodes with degree < 1 can't possibly form triangles.
--
v1_deg_flt = filter v1_degrees by deg_v1 > 1;
v2_deg_flt = filter v2_degrees by deg_v2 > 1;

--
-- Attach the degrees of each vertex to the edges
--
augmented_edges = foreach (join v1_deg_flt by (v1, v2), v2_deg_flt by (v1, v2)) generate
                    v1_deg_flt::v1     as v1,
                    v1_deg_flt::deg_v1 as deg_v1,
                    v2_deg_flt::v2     as v2,
                    v2_deg_flt::deg_v2 as deg_v2;



ordered_edges = foreach augmented_edges {
                  -- order vertices by degree, order lexigraphically in case of tie
                  first  = (deg_v1 <= deg_v2 ? v1 : v2);
                  second = (deg_v1 <= deg_v2 ? v2 : v1);
                  t1     = (deg_v1 == deg_v2 and first > second ? second : first);
                  t2     = (deg_v1 == deg_v2 and first > second ? first  : second);
                  generate
                    t1 as v1,
                    t2 as v2;
                };


edges_g  = group ordered_edges by v1;
edges_gf = foreach (filter edges_g by COUNT(ordered_edges) > 1) generate
             group            as v,
             ordered_edges    as edges,
             ordered_edges.v2 as v1_suggestions, -- yes, using v2 for v1 suggestions is confusing
             ordered_edges.v2 as v2_suggestions;

--
-- Find theoretical closing edges
--
open_triads = foreach edges_gf {
                crossed     = cross v1_suggestions, v2_suggestions;
                suggestions = filter crossed by $0 != $1;

                -- now, dedupe suggestions
                ordered = foreach suggestions generate ($0 > $1 ? $1 : $0) as t1, ($0 > $1 ? $0 : $1) as t2;
                uniq    = distinct ordered;
                generate
                  flatten(uniq)  as (v1_sugg, v2_sugg),
                  edges          as real_edges;
              };

--
-- Last, join with real edges to see what theoretical closing edges
-- actually exist.
--
edges_only = foreach augmented_edges {
               first  = (v1 < v2 ? v1 : v2);
               second = (v1 < v2 ? v2 : v1);
               generate
                 first as v1, second as v2;
             };


triangles = foreach (join open_triads by (v1_sugg, v2_sugg), edges_only by (v1, v2)) generate
              open_triads::v1_sugg    as v1,
              open_triads::v2_sugg    as v2,
              open_triads::real_edges as edges;

dump triangles;
