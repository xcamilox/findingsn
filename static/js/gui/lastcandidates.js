function createPlots(data,layout,itemID){

    // console.log(data)
    // var plot =Plotly.newPlot(document.getElementById(itemID), data, layout, {
    //     scrollZoom: true,
    //     displayModeBar: true,
    //     showlegend: true
    // });
    //
    // document.getElementById(itemID).on('plotly_click', function(data){
    //     for(var i=0; i < data.points.length; i++){
    //         idtosearch=data.points[i].data.marker.ztfid[data.points[i].pointIndex];
    //         //data.points[i].data.ref.search(idtosearch ).draw();
    //         document.dispatchEvent(new CustomEvent("itemSelected",{"detail":idtosearch}))
    //       };
    //
    // });
    // return plot
        var data_series=[]
        for(row of data){
            data_series.push({name:row.name,data:row.val})
        }

        var options = {
              chart: {
              height: 350,
              type: 'scatter',
              animations: {
                  enabled: false,
              },
              zoom: {
                enabled: true,
                type: 'xy'
                },
              },
            xaxis: {
                tickAmount: 10,
                labels: {
                    formatter: function(val) {
                    return parseFloat(val).toFixed(2)
                }
          }
            },
            yaxis: {
                reversed:true,
                forceNiceScale:true,
                decimalsInFloat:1
            },
            series:data_series
        };

        console.log(options)

        var chart = new ApexCharts(document.querySelector("#"+itemID), options);
        chart.render();
}





$(document).ready( function () {


    var opts={}

    if (days!=null){
        opts["days"]=days
    }
    if (filter!=null){
        opts["filter"]=filter
    }
    var request = $.ajax({
            method:"POST",
            url:lastcandidates_url,
            data:opts,
            dataType:"json",
        })
        request.done(function(result){
            if(result.hasOwnProperty("error")){
                $("#error-msg").alert()
            }
            let columns=[]

            let dataset=[]

            for(let row in result){
                let row_data=[]
                for(let col in result[0]){
                    if(col =="lightcurve"){
                        continue;
                    }
                    columns_val=["gmax","rmax","s_gmab","s_rmab","p_rmab","p_gmab","sdis","pdis","photoz","specz"]

                    if (columns_val.includes(col) && result[row][col]!= null && result[row][col]!=""){
                        row_data.push(parseFloat((result[row][col]).toFixed(2)))
                        //row_data.push(result[row][col])

                    }else{
                        row_data.push(result[row][col])
                    }

                }
                dataset.push(row_data)
            }


            for(let col in result[0]){
                if(col =="lightcurve"){
                    continue;
                }
                if(col=="ztfid") {
                    columns.push({
                        title:col, render: function (value, type, row, meta) {
                            return '<a href="https://lasair.roe.ac.uk/object/' + value + '">' + value + '</a>';
                        }
                    })

                }else if (col=="tnsname"){
                    columns.push({
                        title:col, render: function (value, type, row, meta) {
                            if (value != null) {
                                return '<a href="https://wis-tns.weizmann.ac.il/object/' + value + '">' + value + '</a>';
                            }else{
                                return ""
                            }
                        }
                    })
                }
                else if (col=="links"){
                    columns.push({
                        title:col, render: function (value, type, row, meta) {
                            if (value != null) {


                                let links_val = '<a href="http://127.0.0.1:8000/app/object/' + value + '">View Details </a>';


                                return links_val;
                            }else{
                                return ""
                            }
                        }
                    })
                }
                else {
                    columns.push({title: col})
                }

            }

            var reftable = createTable(dataset,columns,"#results-table")
            references["table"] = reftable




            let items={photozr:{x:[],y:[],mag:[],val:[],ztfid:[]},photozg:{x:[],y:[],mag:[],val:[],ztfid:[]},speczr:{x:[],y:[],val:[],mag:[],ztfid:[]},speczg:{x:[],mag:[],y:[],val:[],ztfid:[]}}
            for(let row in result){

                if(result[row]["photoz"]!="" && result[row]["photoz"]>-90){
                    if (result[row]["p_gmab"]!="" && result[row]["p_gmab"]!=null &&result[row]["p_gmab"]<999){
                        items.photozg.val.push([result[row]["photoz"],result[row]["p_gmab"]])
                        items.photozg.x.push(result[row]["photoz"])
                        items.photozg.y.push(result[row]["p_gmab"])
                        items.photozg.mag.push(result[row]["gmax"])
                        items.photozg.ztfid.push(result[row]["ztfid"])
                    }
                    if (result[row]["p_rmab"]!="" && result[row]["p_rmab"]!=null &&result[row]["p_rmab"]<999){
                        items.photozr.val.push([result[row]["photoz"],result[row]["p_rmab"]])
                        items.photozr.x.push(result[row]["photoz"])
                        items.photozr.y.push(result[row]["p_rmab"])
                        items.photozr.mag.push(result[row]["rmax"])
                        items.photozr.ztfid.push(result[row]["ztfid"])
                    }

                }

                 if(result[row]["specz"]!="" && result[row]["specz"]>-90){
                    if (result[row]["s_gmab"]!="" && result[row]["s_gmab"]!=null &&result[row]["s_gmab"]<999){
                        items.speczg.val.push([result[row]["specz"],result[row]["s_gmab"]])
                        items.speczg.x.push(result[row]["specz"])
                        items.speczg.y.push(result[row]["s_gmab"])
                        items.speczg.mag.push(result[row]["gmax"])
                        items.speczg.ztfid.push(result[row]["ztfid"])
                    }
                    if (result[row]["s_rmab"]!="" && result[row]["s_rmab"]!=null &&result[row]["s_rmab"]<999){
                        items.speczr.val.push([result[row]["specz"],result[row]["s_rmab"]])
                        items.speczr.x.push(result[row]["specz"])
                        items.speczr.y.push(result[row]["s_rmab"])
                        items.speczr.mag.push(result[row]["rmax"])
                        items.speczr.ztfid.push(result[row]["ztfid"])
                    }

                }

            }
            plot_data=[]
            idplot="plot_scatter"
            layout = {
                    height:600,
                    xaxis: {
                        title: 'redshift',
                        tickformat: ".2f"
                    },
                    yaxis: {
                        title: 'AB Magnitude',
                        autorange: 'reversed',
                        tickformat: ".2f",
                        showlegend: false
                    },yaxis2: {
                        title: 'Magnitude',
                        autorange: 'reversed',
                        tickformat: ".2f",
                        showlegend: false,
                        overlaying: 'y',
                        side: 'right'
                    },
                    title: "Candidates",
                    showlegend:true,
                    displayModeBar: false

                }
            var g = 'rgb(104,139,46)';
            var r = 'rgb(244,2,52)';
            plot_y_data=[]
            for(let key of Object.keys(items)){
                var color_point = g
                var plot_symbol="circle"
                if(key == "photozr" ||key == "speczr"){
                    color_point = r
                }

                if(key=="speczr" || key=="speczg"){
                    plot_symbol="cross"
                }

                plot_data.push({x:items[key].x,val:items[key].val, y:items[key].y,name:key, type:'scatter',mode:'markers',text:items[key].ztfid, marker:{color:color_point,size:6, ztfid:items[key].ztfid,mag:items[key].mag,symbol:plot_symbol},ref:reftable})

            }
            var plot =createPlots(plot_data,layout,idplot)
            references["plot"]=plot
            references["plotid"]=idplot

            document.dispatchEvent(new CustomEvent("itemSelected",{"detail":dataset[0][0]}))
            references.table.search("").draw()

        })
} );