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
                    
    isComposite: function (graph, u) {
        return 'children' in graph && graph.children(u).length;
    },

    renderGraph: function (graphData, callback) {
        
        var margin = 40;
        var g = graphData.graph;
        var layout = dagreD3.layout().rankSep(60);        
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
            var superGraph = new graphlib.CDigraph();
            var subGraphs = graph.children(null); 
            var nodes = graph.nodes();
            
            var clusters = root.selectAll('g.cluster');
            clusters.each(function (sg) {
                var cluster = d3.select(this);
                var bbox = cluster.node().getBBox();
        
                var xPos = -(bbox.width/2 + margin/2);
                var yPos = -(bbox.height/2 + margin/2);
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
        
                superGraph.addNode(sg, {width: bbox.width+margin, height: bbox.height+margin});                
            });
        
            // Need to add cross cluster edges to supergraph
            graph.eachEdge(function (e, u, v, label) {
                var c0 = graph.parent(u);
                var c1 = graph.parent(v);
                if (c0 != c1) {
                    superGraph.addEdge(null, c0, c1, {minLen: 1});
                }
            });
            
            // layout just supergraph nodes now that we have their dimensions
            var superGraphLayout = dagreD3.layout().run(superGraph);
            superGraph.eachNode(function(u, value) {
                var sn = superGraphLayout.node(u);
                sn.boxW = value.boxW;
                sn.boxH = value.boxH;
            });
            
            // Transform clusters with new layout
            clusters.attr('transform', function (sg) {
                var cluster = superGraphLayout.node(sg);
                var newX = cluster.x+cluster.width/2;
                var newY = cluster.y+cluster.height/2;
                return 'translate('+newX+','+newY+')';
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
            
            function ocalcPoints(e) {
                var value = graph.edge(e);

                var sourceId = graph.incidentNodes(e)[0];
                var targetId = graph.incidentNodes(e)[1];
                
                var source = graph.node(sourceId);
                var target = graph.node(targetId);
            
                var points = value.points.slice();
            
                var p0 = points.length === 0 ? target : points[0];
                var p1 = points.length === 0 ? source : points[points.length - 1];

                points.unshift(intersectRect(source, p0));
                points.push(intersectRect(target, p1));
                
                var clusterSource = graph.parent(sourceId);
                var clusterTarget = graph.parent(targetId);
                if (clusterSource === clusterTarget) {
                    var cluster = superGraphLayout.node(clusterSource);
                    points = points.map(function (point) {
                        point.x = point.x + cluster.x + cluster.width/2;
                        point.y = point.y + cluster.y + cluster.height/2;
                        return point;
                    });
                    
                } else {
                    var c0 = superGraphLayout.node(clusterSource);
                    var c1 = superGraphLayout.node(clusterTarget);
                    var midPoint = {x: (c0.x+c0.width/2 + c1.x+c1.width/2)/2, y: (c0.y+c0.height/2 + c1.y+c1.height/2)/2};
            
                    var newPoints = [];
                    newPoints[0] = {x: points[0].x + c0.x + c0.width/2, y: points[0].y + c0.y + c0.height/2};
                    for (var i = 1; i < points.length - 1; i++) {
                        newPoints[i] = {x: points[i].x + midPoint.x, y: points[i].y + midPoint.y};
                    }
                    newPoints[points.length-1] = {x: points[points.length-1].x + c1.x + c1.width/2, y: points[points.length-1].y + c1.y + c1.height/2};
                    points = newPoints;
                }

                return d3.svg.line()
                    .x(function(d) { return d.x; })
                    .y(function(d) { return d.y; })
                    .interpolate('linear')
                    .tension(0.95)
                (points);
            }
            
            function intersectRect(rect, point) {
                var x = rect.x;
                var y = rect.y;
                // For now we only support rectangles
            
                // Rectangle intersection algorithm from:
                // http://math.stackexchange.com/questions/108113/find-edge-between-two-boxes
                var dx = point.x - x;
                var dy = point.y - y;
                var w = rect.width / 2;
                var h = rect.height / 2;
            
                var sx, sy;
                if (Math.abs(dy) * w > Math.abs(dx) * h) {
                    // Intersection is top or bottom of rect.
                    if (dy < 0) {
                        h = -h;
                    }
                    sx = dy === 0 ? 0 : h * dx / dy;
                    sy = h;
                } else {
                    // Intersection is left or right of rect.
                    if (dx < 0) {
                        w = -w;
                    }
                    sx = w;
                    sy = dx === 0 ? 0 : w * dy / dx;
                }
                return {x: x + sx, y: y + sy};
            }

            function moveEdgeLabel(e) {
                var value = graph.edge(e);
                var sourceId = graph.incidentNodes(e)[0];
                var targetId = graph.incidentNodes(e)[1];
                var clusterSource = graph.parent(sourceId);
                var clusterTarget = graph.parent(targetId);
                
                var points = value.points.slice();
            
                var p0 = points.length === 0 ? target : points[0];
                var p1 = points.length === 0 ? source : points[points.length - 1];
                if (clusterSource === clusterTarget) {
                    var cluster = superGraphLayout.node(clusterSource);
                    points = points.map(function (point) {
                        point.x = point.x + cluster.x + cluster.width/2;
                        point.y = point.y + cluster.y + cluster.height/2;
                        return point;
                    });
                } else {
                    var c0 = superGraphLayout.node(clusterSource);
                    var c1 = superGraphLayout.node(clusterTarget);
                    var midPoint = {x: (c0.x+c0.width/2 + c1.x+c1.width/2)/2, y: (c0.y+c0.height/2 + c1.y+c1.height/2)/2};
            
                    var newPoints = [];
                    newPoints[0] = {x: points[0].x + c0.x + c0.width/2, y: points[0].y + c0.y + c0.height/2};
                    for (var i = 1; i < points.length - 1; i++) {
                        newPoints[i] = {x: points[i].x + midPoint.x, y: points[i].y + midPoint.y};
                    }
                    newPoints[points.length-1] = {x: points[points.length-1].x + c1.x + c1.width/2, y: points[points.length-1].y + c1.y + c1.height/2};
                    points = newPoints;                                        
                }

                var point = findMidPoint(points);
                return 'translate(' + point.x + ',' + point.y + ')';
            }            
            
            svgEdgePaths.selectAll('path')
                .attr('d', ocalcPoints);

             svgEdgeLabels.selectAll('g.edgeLabel').each(function (e) {
                 var edgeLabel = d3.select(this);
                 var transform = edgeLabel.attr('transform');
                 var points = transform.substring(10, transform.length-1).split(",").map(function(p) {return parseFloat(p);});

                 var sourceId = graph.incidentNodes(e)[0];
                 var targetId = graph.incidentNodes(e)[1];
                 var clusterSource = graph.parent(sourceId);
                 var clusterTarget = graph.parent(targetId);

                 if (clusterSource === clusterTarget) {
                     var cluster = superGraphLayout.node(clusterSource);
                     var newX = points[0] + cluster.x + cluster.width/2;
                     var newY = points[1] + cluster.y + cluster.height/2;
                     edgeLabel.attr('transform', 'translate('+newX+','+newY+')');
                 } else {
                     var c0 = superGraphLayout.node(clusterSource);
                     var c1 = superGraphLayout.node(clusterTarget);
                     var midPoint = {x: (c0.x+c0.width/2 + c1.x+c1.width/2)/2, y: (c0.y+c0.height/2 + c1.y+c1.height/2)/2};
            
                     var newX = points[0] + midPoint.x;
                     var newY = points[1] + midPoint.y;
                     edgeLabel.attr('transform', 'translate('+newX+','+newY+')');
                 }
             });
            
            oldPostRender(graph, root);            
        });
        
        var svg = d3.select('#pig-graph').append('svg').append('g');                
        renderer.run(g, svg);

        var bbox = d3.select('svg').node().getBBox();
        var viewHeight = bbox.height*2+"pt";
        var viewWidth = bbox.width*2+"pt";
        var result = "<svg height=\""+viewHeight+"\" width=\""+viewWidth+"\">"+svg.html()+"</svg>";

        callback(result, renderer);
    }
};
