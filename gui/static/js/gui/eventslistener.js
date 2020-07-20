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


function plotPositionDispersion(ra,dec,g_detec,r_detec,divid) {
    pos = getPositions(ra,dec,g_detec,r_detec)

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

function getPositions(ra,dec,ra_detec,dec_detec) {
    first_ra = Number(ra) * 3600;
    first_dec = Number(dec) * 3600;


    g = ra_detec
    r = dec_detec

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


function updateLinks(ra, dec, ztfid) {

    lasair_val = 'https://lasair.roe.ac.uk/object/' + ztfid;
    $("#lasair_url").attr("href", lasair_val)
    alerce_val = 'https://alerce.online/object/' + ztfid;
    $("#alerce_url").attr("href", alerce_val)
    decals_val = 'http://legacysurvey.org/viewer?ra=' + ra + '&dec=' + dec + '&zoom=14&spectra&mark=' + ra + ',' + dec;
    $("#decals_url").attr("href", decals_val)
    sdss_val = 'http://skyserver.sdss.org/dr16/en/tools/quicklook/summary.aspx?ra=' + ra + '&dec=' + dec;
    $("#sdss_url").attr("href", sdss_val)
    pansatars_val = 'http://ps1images.stsci.edu/cgi-bin/ps1cutouts?pos=' + ra + '%2B' + dec + '&filter=color';
    $("#panstars_url").attr("href", pansatars_val)
    ned_val = 'http://ned.ipac.caltech.edu/cgi-bin/nph-objsearch?lon=' + ra + 'd&lat=' + dec + 'd&radius=2.0&search_type=Near+Position+Search';
    $("#ned_url").attr("href", ned_val)

    pacs_val="https://sky.esa.int/?target="+ra+"%20"+dec+"&hips=Herschel+PACS+RGB+70%2C+160+micron&fov=0.18749936600501044&cooframe=J2000&sci=true&lang=en"
    $("#pacs_url").attr("href", pacs_val)
    spire_val="https://sky.esa.int/?target="+ra+"%20"+dec+"&hips=Herschel+SPIRE+RGB+250%2C+350%2C+500+micron&fov=0.4990679021341206&cooframe=J2000&sci=true&lang=en"
    $("#spire_url").attr("href", spire_val)
    "https://wis-tns.weizmann.ac.il/search?ra="+ra+"&decl="+dec+"&radius=5&coords_unit=arcsec"
    //tns_val = '<a href="https://wis-tns.weizmann.ac.il/object/' + value + '">' + value + '</a>'
    tns_val="https://wis-tns.weizmann.ac.il/search?ra="+ra+"&decl="+dec+"&radius=5&coords_unit=arcsec";
    $("#tns_url").attr("href", tns_val)
}

function createPlot(data, title, idelement) {

    lightcurveplot = document.getElementById(idelement);
    layout = {
        width: 350,
        xaxis: {
            title: 'MJD',
            tickformat: ".f"
        },
        yaxis: {
            title: 'Difference Magnitude',
            autorange: 'reversed',
            //tickformat: ".f",
            showlegend: false
        },
        //title: title,
        showlegend: false,
        displayModeBar: false

    }

    Plotly.newPlot(lightcurveplot, data, layout, {
        scrollZoom: false,
        displayModeBar: false,
        showlegend: false
    });
}

function selecteItem(ztfid) {
    //update selected figure
    // update title
    // update aladin
    //update light curve
    //select table

    //console.log("item selected",ztfid)


    references.table.search(ztfid).draw()
    item = references.table.rows({filter: 'applied'}).data()[0]
    let ra= item[raidx.idx]
    let dec=item[decidx.idx]
    console.log("selected",ztfid,ra,dec)
    references.aladin.gotoRaDec(ra, dec);
    loadCatalogs(ra,dec)
    $("#currentItemTitle").html(ztfid)
    updateLinks(ra, dec, ztfid)

    var plot = document.getElementById('references.plotid')

    var request = $.ajax({
        method: "POST",
        url: lightcurve_url,
        data: {"ztfid": ztfid},
        dataType: "json",
    })
    request.done(function (result) {

        //index
        // 0: "oid"
        // 1: "candid"
        // 2: "mjd"
        // 3: "fid"
        // 4: "diffmaglim"
        // 5: "magpsf"
        // 6: "magap"
        // 7: "sigmapsf"
        // 8: "sigmagap"
        // 9: "ra"
        // 10: "dec"
        // 11: "sigmara"
        // 12: "sigmadec"
        // 13: "isdiffpos"
        // 14: "distpsnr1"
        // 15: "sgscore1"
        // 16: "field"
        // 17: "rcid"
        // 18: "magnr"
        // 19: "sigmagnr"
        // 20: "rb"
        // 21: "magpsf_corr"
        // 22: "magap_corr"
        // 23: "sigmapsf_corr"
        // 24: "sigmagap_corr"
        // 25: "has_stamps"
        // 26: "parent_candid"


        var g_detections={mjd:[],mag:[],magerr:[],ra:[],dec:[]}
        var r_detections={mjd:[],mag:[],magerr:[],ra:[],dec:[]}
        for(let item in result.data){
            let row=result.data[item];
            if(row[3]==1){
                g_detections.mjd.push(row[2])
                g_detections.mag.push(row[6])
                g_detections.magerr.push(row[8])
                g_detections.ra.push(row[9])
                g_detections.dec.push(row[10])
            }else{
                r_detections.mjd.push(row[2])
                r_detections.mag.push(row[6])
                r_detections.magerr.push(row[8])
                r_detections.ra.push(row[9])
                r_detections.dec.push(row[10])
            }
        }
        var data = []
        if(g_detections.mjd.length>0){
            data.push(plotLightCurve(g_detections.mjd, g_detections.mag, g_detections.magerr, 1))
        }
        if (r_detections.mjd.length>0) {
            data.push(plotLightCurve(r_detections.mjd, r_detections.mag, r_detections.magerr, 2))


        }

        createPlot(data, ztfid, "lightcurve")
        plotPositionDispersion(ra,dec,g_detections,r_detections,"position-plot")

    })

}

document.addEventListener("itemSelected",
    function (event) {

        selecteItem(event.detail)
    }, false)