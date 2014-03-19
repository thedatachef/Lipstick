;GraphRenderer = {    
    addLabel: function(node, root, viewModel) {
        var labelSvg = root.append('g');
        
        var fo = labelSvg
            .append('foreignObject')
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
        var graph = graphData.graph;
        var layout = dagreD3.layout().rankSep(60);
        var renderer = new dagreD3.Renderer().layout(layout);
        var oldDrawEdges = renderer.drawEdgePaths();
        renderer.drawNodes(function(graph, root) {
          var nodes = graph.nodes().filter(function(u) { return !GraphRenderer.isComposite(graph, u); });
          
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
        
        var svg = d3.select('#pig-graph').append('svg').append('g');        
        renderer.edgeInterpolate('linear');
        renderer.edgeTension(1.0);
        renderer.run(graph, svg);
        
        //
        // Post processing; add rectangles
        //
        var viewMargin = 20;
        var subGraphs = graph.children(null); 
        var nodes = graph.nodes().filter(function(u) { return !GraphRenderer.isComposite(graph, u); });
        for (var i = 0; i < subGraphs.length; i++) {
            var subBox = {maxX:null, maxY:null, minX:null, minY:null};

            var subNodes = graph.children(subGraphs[i]);
            var subSelect = d3.selectAll("g.node").data(nodes, function(u) {return u;})
                .filter(function(u) { return (subNodes.indexOf(u) >= 0); });

            subSelect.each(function(u) {
              var nodeBox = d3.select(this).node().getBBox();              
              var transform = d3.select(this).attr("transform");
              var points = transform.substring(10, transform.length-1).split(",").map(function(p) {return parseFloat(p);});                               

              var right = points[0]+nodeBox.width/2;
              var left = points[0]-nodeBox.width/2;                                
              var bottom = points[1]+nodeBox.height/2;
              var top = points[1]-nodeBox.height/2;

              if (right > subBox.maxX || subBox.maxX == null) { subBox.maxX = right; }
              if (left < subBox.minX || subBox.minX == null) { subBox.minX = left; }
              if (bottom > subBox.maxY || subBox.maxY == null) { subBox.maxY = bottom; }
              if (top < subBox.minY || subBox.minY == null) { subBox.minY = top; }
            });

            // Get view model data for cluster            
            var clusterData = graph.node(subGraphs[i]).data;

            var cluster = svg.insert('g', ':first-child')
                .attr('id', subGraphs[i])
                .attr('data-bind', 'attr {class: clusters[\''+subGraphs[i]+'\'].css}');

            cluster
                .append('title')
                    .text(subGraphs[i].split('-')[1]);
                        
            cluster
                .append('rect')
                    .attr('x', subBox.minX - viewMargin)
                    .attr('y', subBox.minY - viewMargin)
                    .attr('width', subBox.maxX - subBox.minX + viewMargin*2)
                    .attr('height', subBox.maxY - subBox.minY + viewMargin*2)
                    .attr('fill', '#e9e9e9')
                    .attr('stroke', 'black')
                    .attr('stroke-width', '1.5px')
                    .style('opacity', 0.6);
            
        }

        var bbox = svg.node().getBBox();
        var viewHeight = bbox.height+"pt";
        var viewWidth = bbox.width+"pt";
        var viewBox = (-viewMargin)+" "+(-viewMargin)+" "+bbox.width+" "+(bbox.height+100);
        var result = "<svg height=\""+viewHeight+"\" width=\""+viewWidth+"\" viewBox=\""+viewBox+"\">"+svg.html()+"</svg>";

        callback(result, renderer);
    }
};
