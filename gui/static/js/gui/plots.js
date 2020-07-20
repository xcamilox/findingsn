function plotDetections(value,fid,id){
    g = 'rgb(104,139,46)';
    r = 'rgb(244,2,52)';
    point_color=["",g,r]
    return {
            x: _.pluck(value,"mjd"),
            y: _.pluck(value,"magpsf"),
            marker: { color:point_color[fid] ,size:10},
            type: 'scatter',
            mode: 'markers',
            customdata:{imgref:id,reference:_.pluck(value,"candid")},
            error_y: {
              type: 'data',
              array: _.pluck(value,"sigmapsf"),
              visible: true,
              color: point_color[fid],
              opacity: 0.7,

            }
          }
}
function plotNodetections(value,fid){
    ng = 'rgb(216,237,207)';
    nr = 'rgb(255,209,209)';
    nodetections_color=["",ng,nr]
    return {
            x: _.pluck(value,"mjd"),
            y: _.pluck(value,"magpsf"),
            opacity:0.5,
            marker: { color:nodetections_color["fid"], symbol:"diamond" ,size:6},
            type: 'scatter',
            mode: 'markers'

          }
}

function getData(source){

    var data = [];

    detections={"x":[],"y":[],"fid":[],"id":[],"ref":[]}
    nodetections={"g":[],"r":[]}

    detections =  datasource.lightcurve.map(function(item){if(item.hasOwnProperty("drb") || item.hasOwnProperty("rb")){return item;}}).filter(cand => cand != undefined)
    nodetections =  datasource.lightcurve.map(function(item){if(!item.hasOwnProperty("drb") && !item.hasOwnProperty("rb")){return item;}}).filter(cand => cand != undefined)

    //g detections
    data.push(plotDetections(_.where(detections,{"fid":1}),1,datasource.id))
    //r detections
    data.push(plotDetections(_.where(detections,{"fid":2}),2,datasource.id))
    //g non detections
    data.push(plotNodetections(_.where(nodetections,{"fid":1}),1))
    //r non detections
    data.push(plotNodetections(_.where(nodetections,{"fid":2}),2))

    return data
}

function getPositions(data){
    first_ra = Number(data.ra)*3600;
    first_dec = Number(data.dec)*3600;

    detections =  data.lightcurve.map(function(item){if(item.hasOwnProperty("drb") || item.hasOwnProperty("rb")){return item;}}).filter(cand => cand != undefined)
    console.log(detections)
    g=_.where(detections,{"fid":1})
    r=_.where(detections,{"fid":2})
    console.log(g,r)
    gra=_.pluck(g,"ra").map(function(val){return first_ra - Number(val)*3600})
    rra=_.pluck(r,"ra").map(function(val){return first_ra - Number(val)*3600})
    gdec=_.pluck(g,"decl").map(function(val){return first_dec - Number(val)*3600})
    rdec=_.pluck(r,"decl").map(function(val){return first_dec - Number(val)*3600})
    console.log({"gra":gra,"gdec":gdec,"rra":rra,"rdec":rdec})
    return {"gra":gra,"gdec":gdec,"rra":rra,"rdec":rdec}
}

$(document).ready( function () {



    lightcurveplot = document.getElementById('lightcurve');
    layout = {
        width: 700,
        xaxis: {
            title: 'MJD',
            tickformat: ".f"
        },
        yaxis: {
            title: 'Difference Magnitude',
            autorange: 'reversed',
            tickformat: ".f",
            showlegend: false
        },
        title: datasource.id,
        showlegend:false,
        displayModeBar: false

    }

    Plotly.newPlot(lightcurveplot, getData(datasource), layout, {
        scrollZoom: false,
        displayModeBar: false,
        showlegend: false
    });
    positionsplot = document.getElementById('positions');


    pos=getPositions(datasource)

    var radecg = {x:pos.gra, y: pos.gdec,
    mode:'markers',
    marker: { color:'rgb(104,139,46)' },
    type:'scatter'
}

    var radecr = {x:pos.rra, y: pos.rdec,
        mode:'markers',
        marker: { color:'rgb(244,2,52)' },
        type:'scatter'
    }

    Plotly.plot(positionsplot, [radecg, radecr], {
        margin: { t: 0 },
        showlegend: false,
            width: 370,
            height: 285,
        shapes: [
            {
                type: 'circle',
                xref: '0',
                yref: '0',
                x0: -1.5,
                y0: -1.5,
                x1: 1.5,
                y1: 1.5,
                opacity: 0.3,
                fillcolor: '#bbded6',
                line: {
                    color: 'black'
                }
            }]
    }, {displayModeBar: false}
    );
})
