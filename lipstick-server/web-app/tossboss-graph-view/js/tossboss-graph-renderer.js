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
               // Apply bindings here so html is rendered and the bbox 
               // can be computed
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
        var layout = dagreD3.layout();
        var renderer = new dagreD3.Renderer().layout(layout);
        var oldDrawEdges = renderer.drawEdgePaths();
        var oldPostRender = renderer.postRender();

        renderer.drawNodes(function(graph, root) {
            var subGraphs = graph.children(null);

            var nodeGroups = root.selectAll('g.cluster')
                .data(subGraphs, function (sg) { return sg; });

            // Remove any existing cluster data
            nodeGroups.selectAll('*').remove();
            nodeGroups
                .enter()
                .append('g')
                .classed('cluster', true);

            nodeGroups.each(function (sg) {

                var nodes = graph.nodes().filter(function(u) { return graph.parent(u) === sg; });

                var cluster = d3.select(this);
                var svgNodes = cluster
                    .selectAll('g.node')
                    .data(nodes, function(u) { return u; });
          
                svgNodes.selectAll('*').remove();
          
                svgNodes
                    .enter()
                    .append('g')
                    .attr('class', 'node');

                // Might be a performance hit to send whole viewmodel each time
                svgNodes.each(function(u) { GraphRenderer.addLabel(graph.node(u), d3.select(this), graphData.viewModel); });
                svgNodes.attr('id', function(u) { return u; });
            });
            
            var svgNodes = root.selectAll('g.node');
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
        
        renderer.postRender(function (graph, root) {
            var subGraphs = graph.children(null); 
            var nodes = graph.nodes();
            
            var clusters = root.selectAll('g.cluster');
            clusters.each(function (sg) {
                var cluster = d3.select(this);
                var bbox = cluster.node().getBBox();
        
                cluster
                    .attr('id', sg)
                    .attr('data-bind',
                      'attr {class: clusters[\''+sg+'\'].css},'+
                      'click: clickedCluster,' +
                      'clickBubble: false, '+
                      'event: { mouseover: mouseEnterCluster, mouseout: mouseLeaveCluster }'
                     )
                    .append('title')
                    .text(sg.split('-')[1]);
                
                cluster
                    .insert('rect', ':first-child')                    
                    .attr('x', bbox.x-margin/2)
                    .attr('y', bbox.y-margin/2)
                    .attr('width', bbox.width+margin)
                    .attr('height', bbox.height+margin)
                    .attr('fill', '#e9e9e9')
                    .attr('stroke', 'black')
                    .attr('stroke-width', '1.5px')
                    .style('opacity', 0.6);
        
            });
                                
            d3.selection.prototype.moveToFront = function() {
                return this.each(function(){
                    this.parentNode.appendChild(this);
                });
            };
            
            var svgEdgeLabels = svg.select('g.edgeLabels');
            var svgEdgePaths = svg.select('g.edgePaths');
            svgEdgePaths.moveToFront();
            svgEdgeLabels.moveToFront();
            
            oldPostRender(graph, root);            
        });
        
        var svg = d3.select('#pig-graph').append('svg').append('g');                
        renderer.edgeInterpolate('linear');
        renderer.run(g, svg);

        var bbox = d3.select('svg').node().getBBox();
        var viewHeight = bbox.height*2+"pt";
        var viewWidth = bbox.width*2+"pt";
        var result = "<svg height=\""+viewHeight+"\" width=\""+viewWidth+"\">"+svg.html()+"</svg>";

        callback(result, renderer);
    }
};
