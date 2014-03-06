;GraphRenderer = {
    
    options: {
        bgCluster: "#E9E9E9",
        bgAlias: "#424242",
        bgExpression: "#BCBCBC",
        bgMapTask: "#3299BB",
        bgRedTask: "#FF9900",
        bgUnkTask: "#BF0A0D",
        bgWhite: "#FFFFFF"
    },

    getJobColor: function(node) {
        console.log(node.mapReduce.stepType());
        if (node.mapReduce) {
            var stepType = node.mapReduce.stepType();
            if (stepType === "MAPPER") {
                return GraphRenderer.options.bgMapTask;
            }
            if (stepType === "REDUCER") {
                return GraphRenderer.options.bgRedTask;
            }
        }
        return GraphRenderer.options.bgUnkTask;
    },

    schemaEqualsPredecessor: function(node, graph) {
        if (node.schemaString) {
            var operString = node.schemaString.substring(1, node.schemaString.length - 1);
            var preds = graph.predecessors(node.uid);
            for (var i = 0; i < preds.length; i++) {                
                var pred = graph.node(preds[i]);
                if (pred.schemaString) {
                    var predString = pred.schemaString().substring(1, pred.schemaString().length - 1);
                    // Ouch, just do string comparison for now; no deep compare
                    return (predString === operString);
                } 
            }
            return true;
        }
        return false;
    },

    displaySchema: function(node) {
        if (node.location.line != null
            && !GraphRenderer.schemaEqualsPredecessor(node)
            && !node.operator.equalsIgnoreCase("LOSplit")
            && !node.operator.equalsIgnoreCase("LOFilter")
            && !node.operator.equalsIgnoreCase("LODistinct")
            && !node.operator.equalsIgnoreCase("LOLimit")
            && !node.operator.equalsIgnoreCase("LOJoin")
            && !node.operator.equalsIgnoreCase("LOCogroup")) {
            return true;
        }
        return false;
    },

    nodeViewData: function(node, h) {
        var data = [];
        data.push({x:0, y:-h, height: h, text: node.operator, color: GraphRenderer.getJobColor(node)});
        data.push({x:0, y:0, height: h, text: node.alias, color: GraphRenderer.options.bgAlias});
        return data;
    },

    addNode: function(node, root, marginX, marginY) {
        var rectSvg = root.append('rect');
        var nodeSvg = root.append('g');
        var rectH = 30;
        
        var data = GraphRenderer.nodeViewData(node, rectH);
        
        var labelGroup = nodeSvg.selectAll("g")
                           .data(data).enter().append("g");
        var rect = labelGroup.append("rect");
        var text = labelGroup.append("text");

        // Layout the text labels and get max label width 
        var maxWidth = 0;
        text
          .text(function(data) { return data.text; })
          .attr('text-anchor', 'end')
          .attr('font-family', 'Times,serif')
          .attr('font-size', '12.00')
          .attr('y', function(data) { return data.y + data.height/2; })
          .each(function(data) { 
            var w = d3.select(this).node().getBBox().width; 
            if (w > maxWidth) { maxWidth = w; }
          });
        
        rect
          .attr('x', -(maxWidth/2+marginX) )
          .attr('y', function(data) { return data.y; } )
          .attr('width', maxWidth+2*marginX)
          .attr('height', rectH)
          .attr('fill', function(data) { return data.color; });
        
        // Align text in the center
        text.attr('transform', function(data) {
          var w = d3.select(this).node().getComputedTextLength();
          return 'translate('+w/2+',0)';
        });

        var bbox = root.node().getBBox();
        rectSvg
          .attr('x', -(bbox.width / 2))
          .attr('y', -rectH)
          .attr('width', bbox.width)
          .attr('height', bbox.height);        
    },
                    
    isComposite: function (graph, u) {
        return 'children' in graph && graph.children(u).length;
    },

    renderGraph: function (graphData) {        
        var renderer = new dagreD3.Renderer();
        var oldDrawNodes = renderer.drawNodes();        
        renderer.drawNodes(function(graph, root) {
          var nodes = graph.nodes().filter(function(u) { return !GraphRenderer.isComposite(graph, u); });
          
          var svgNodes = root
              .selectAll('g.node')
              .classed('enter', false)
              .data(nodes, function(u) { return u; });
          
          svgNodes.selectAll('*').remove();
          
          svgNodes
              .enter()
              .append('g')
              .style('opacity', 0)
              .attr('class', 'node enter');
          
          svgNodes.each(function(u) { GraphRenderer.addNode(graph.node(u), d3.select(this), 10, 10); });          
          svgNodes.attr('id', function(u) { return u; });
                               
          return svgNodes;
        });
        
        renderer.run(graphData.graph, d3.select("svg g"));
        ko.applyBindings(graphData.viewModel); // no bindings yet...
    }
};
