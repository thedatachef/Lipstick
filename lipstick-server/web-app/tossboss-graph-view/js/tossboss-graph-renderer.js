;GraphRenderer = {    
    addNode: function(node, root) {
               
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

    renderGraph: function (graph, callback) {        
        var renderer = new dagreD3.Renderer();
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
                               
          svgNodes.each(function(u) { GraphRenderer.addNode(graph.node(u), d3.select(this)); });          
          svgNodes.attr('id', function(u) { return u; });

          return svgNodes;
        });
        
        var svg = d3.select('#pig-graph').append('svg').append('g');
        renderer.edgeInterpolate('linear')
        renderer.run(graph, svg);

        var bbox = svg.node().getBBox();        
        var viewHeight = bbox.height+"pt";
        var viewWidth = bbox.width+"pt";
        var viewBox = "0.00 0.00 "+bbox.width+" "+bbox.height;
        var result = "<svg height=\""+viewHeight+"\" width=\""+viewWidth+"\" viewBox=\""+viewBox+"\">"+svg.html()+"</svg>";

        callback(result);
    }
};
