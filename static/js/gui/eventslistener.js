function plotLightCurve(x,y,error,fid){
    g = 'rgb(104,139,46)';
    r = 'rgb(244,2,52)';
    point_color=["",g,r]
    return {
            x: x,
            y: y,
            marker: { color:point_color[fid] ,size:10},
            type: 'scatter',
            mode: 'markers',
            tickformat: ".f",
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

function updateLinks(ra,dec,ztfid){

    lasair_val = 'https://lasair.roe.ac.uk/object/' + ztfid;
    $("#lasair_url").attr("href",lasair_val)
    alerce_val = 'https://alerce.online/object/' + ztfid;
    $("#alerce_url").attr("href",alerce_val)
    decals_val = 'http://legacysurvey.org/viewer?ra='+ra+'&dec='+dec+'&zoom=14&mark='+ra+','+dec;
    $("#decals_url").attr("href",decals_val)
    sdss_val = 'http://skyserver.sdss.org/dr16/en/tools/quicklook/summary.aspx?ra='+ra+'&dec='+dec;
    $("#sdss_url").attr("href",sdss_val)
    pansatars_val = 'http://ps1images.stsci.edu/cgi-bin/ps1cutouts?pos='+ra+'%2B'+dec+'&filter=color';
    $("#panstars_url").attr("href",pansatars_val)
    ned_val = 'http://ned.ipac.caltech.edu/cgi-bin/nph-objsearch?lon='+ra+'d&lat='+dec+'d&radius=2.0&search_type=Near+Position+Search';
    $("#ned_url").attr("href",ned_val)
}

function createPlot(data,title,idelement){

    lightcurveplot = document.getElementById(idelement);
    layout = {
        width: 400,
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
        title: title,
        showlegend:false,
        displayModeBar: false

    }

    Plotly.newPlot(lightcurveplot, data, layout, {
        scrollZoom: false,
        displayModeBar: false,
        showlegend: false
    });
}

function selecteItem(ztfid){
    //update selected figure
    // update title
    // update aladin
    //update light curve
    //select table

    //console.log("item selected",ztfid)

    references.table.search(ztfid).draw()
    item=references.table.rows( { filter : 'applied'} ).data()[0]
    references.aladin.gotoRaDec(item[2],item[3]);
    $("#currentItemTitle").html(ztfid)
    updateLinks(item[2],item[3],ztfid)
    var plot = document.getElementById('references.plotid')

    var request = $.ajax({
            method:"POST",
            url:lightcurve_url,
            data:{"ztfid":ztfid},
            dataType:"json",
        })
        request.done(function(result){

            var items = result[0].lightpeak.lightcurve
            var data=[]
            if(items.hasOwnProperty("g")){
                var g_detections = items.g
                data.push(plotLightCurve(g_detections.mjd,g_detections.mag,g_detections.magerr,1))

            }
            if(items.hasOwnProperty("r")){
                var r_detections = items.r
                data.push(plotLightCurve(r_detections.mjd,r_detections.mag,r_detections.magerr,2))


            }


            createPlot(data,ztfid,"lightcurve")


        })

}

document.addEventListener("itemSelected",
    function (event){

        selecteItem(event.detail)
    },false)