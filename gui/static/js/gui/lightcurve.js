function plotLightCurve(x, y, error, fid) {
    g = 'rgb(104,139,46)';
    r = 'rgb(244,2,52)';
    point_color = ["", g, r]
    return {
        x: x,
        y: y,
        marker: {color: point_color[fid], size: 10},
        type: 'scatter',
        mode: 'markers',
        tickformat: ".2f",
        //customdata:{imgref:id,reference:_.pluck(value,"candid")},
        error_y: {
            type: 'data',
            array: error,
            visible: true,
            color: point_color[fid],
            opacity: 0.7,

        }
    }
}
function plotPositionDispersion(ra,dec,lightcurve,divid) {
    pos = getPositions(ra,dec,lightcurve)

    var radecg = {
        x: pos.gra, y: pos.gdec,
        mode: 'markers',
        marker: {color: 'rgb(104,139,46)'},
        type: 'scatter'
    }

    var radecr = {
        x: pos.rra, y: pos.rdec,
        mode: 'markers',
        marker: {color: 'rgb(244,2,52)'},
        type: 'scatter'
    }

    Plotly.newPlot(document.getElementById(divid), [radecg, radecr], {
            margin: {t: 5},
            showlegend: false,
            width: 350,
            height: 270,
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

}

function getPositions(ra,dec,lightcurve) {
    first_ra = Number(ra) * 3600;
    first_dec = Number(dec) * 3600;


    g = lightcurve.g
    r = lightcurve.r

    gra = g.ra.map(function (val) {
        console.log(val)
        return first_ra - Number(val) * 3600
    })
    rra = r.ra.map(function (val) {
        return first_ra - Number(val) * 3600
    })
    gdec = g.dec.map(function (val) {
        return first_dec - Number(val) * 3600
    })
    rdec = r.dec.map(function (val) {
        return first_dec - Number(val) * 3600
    })

    return {"gra": gra, "gdec": gdec, "rra": rra, "rdec": rdec}
}

