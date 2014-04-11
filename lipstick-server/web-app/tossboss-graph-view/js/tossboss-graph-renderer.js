;GraphRenderer = {
                                        
    addLabel: function(node, root, viewModel) {
        var labelSvg = root.append('g');
        var fo = labelSvg
            .append('foreignObject')
            .classed('foreign-html', true)
            .classed(node.type+'-'+node.id.replace('>',''), true) 
            .attr('width', '100000');

        var w, h;
        fo
            .append('xhtml:div')
            .style('float', 'left')
            .html(function() { return node.label; })
            .each(function() {               
               // So dimensions can be computed
               ko.applyBindings(viewModel, d3.select(this).node());
               w = this.clientWidth;
               h = this.clientHeight;
            });

        fo
            .attr('width', w)
            .attr('height', h);
        
        var bbox = root.node().getBBox();
        labelSvg.attr('transform',
             'translate(' + (-bbox.width / 2) + ',' + (-bbox.height / 2) + ')');
    },
                    
    renderGraph: function (graphData, callback) {
        
        var margin = 40;
        var g = graphData.graph;
        var renderer = new dagreD3.Renderer();
        var oldDrawEdges = renderer.drawEdgePaths();
        var oldDrawClusters = renderer.drawClusters();

        renderer.drawNodes(function(graph, root) {

            var nodes = graph.nodes().filter(function(u) {return !('children' in g && g.children(u).length)});

            var svgNodes = root
                .selectAll('g.node')
                .data(nodes, function(u) { return u; });
          
            svgNodes.selectAll('*').remove();
          
            svgNodes
                .enter()
                .append('g')
                .attr('class', 'node');

            svgNodes.each(function(u) { GraphRenderer.addLabel(graph.node(u), d3.select(this), graphData.viewModel); });
            svgNodes.attr('id', function(u) { return u; });
            return svgNodes;
        });
            
        renderer.drawEdgePaths(function (graph, root) {
            var edgePaths = oldDrawEdges(graph, root);
            edgePaths.attr('id', function(edge, idx) { return "edge"+idx; });
            edgePaths.attr('data-start', function(edge, idx) {
                return edge.split('->')[0];
            });
            edgePaths.attr('data-end', function(edge, idx) {
                return edge.split('->')[1];
            });
            edgePaths.classed('edge', true);
            edgePaths.append('title').text(function(edge) { return edge;});
        
            return edgePaths;                                   
        });

        renderer.drawEdgeLabels(function (graph, root) {
            var svgEdgeLabels = root
                .selectAll('g.edgeLabel')
                .classed('enter', false)
                .data(graph.edges(), function (e) { return e; });
        
            svgEdgeLabels.selectAll('*').remove();
        
            svgEdgeLabels
                .enter()
                .append('g')
                .attr('class', 'edgeLabel enter');
        
            svgEdgeLabels.each(function(e) { GraphRenderer.addLabel(graph.edge(e), d3.select(this), graphData.viewModel); });
        
            return svgEdgeLabels;
        });

        renderer.drawClusters(function (graph, root) {
            var svgClusters = oldDrawClusters(graph, root);
            
            svgClusters
                .attr('id', function(sg) { return sg; })
                .attr('data-bind', function(sg) {
                      return 'attr {class: clusters[\''+sg+'\'].css},'+
                             'click: clickedCluster,' +
                             'clickBubble: false, '+
                             'event: { mouseover: mouseEnterCluster, mouseout: mouseLeaveCluster }'
                })
                .append('title')
                .text(function(sg) { return sg.split('-')[1]; } );
            return svgClusters;
        });

        var pigGraph = d3.select('#pig-graph');
        pigGraph.selectAll('*').remove();
        
        var svg = pigGraph.append('svg');
        renderer.run(g, svg.append('g'));

        var bbox = svg.node().getBBox();
        
        svg
            .attr('width', bbox.width+0.1*bbox.width)
            .attr('height', bbox.height+0.1*bbox.height);
            
        svg.select('g').attr('transform', 'translate('+0+','+0.01*bbox.height+')');

        // Hack for knockout bindings to work; expensive?
        var result = pigGraph.html();
        pigGraph.html(result);                
        
        ko.applyBindings(graphData.viewModel, pigGraph.node());
    }
};
