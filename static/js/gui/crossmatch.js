$(document).ready( function () {

    var buttonStatus="<span class=\"spinner-border spinner-border-sm\" role=\"status\" aria-hidden=\"true\"></span>Loading..."

    function getSimplewrap(id){
        var singleElemt='<div class="card">\n' +
        '\t<div class="card-header" id="heading'+id+'">\n' +
        '\t  <h5 class="mb-0">\n' +
        '\t    <button class="btn btn-link collapsed" data-target="#'+id+'" data-toggle="collapse" aria-expanded="false" aria-controls="collapse'+id+'">\n' +
        '\t      id\n' +
        '\t    </button>\n' +
        '\t  </h5>\n' +
        '\t</div>\n' +
        '\t<div id="'+id+'" class="collapse" aria-labelledby="heading'+id+'" data-parent="#accordion">\n' +
        '\t  <div class="card-body">\n' +
        '\t    <div class="contenttree"></div>\n' +
        '\t  </div>\n' +
        '\t</div>\n' +
        '</div>'
        return singleElemt
    }

    $("#crossmatch-form").submit(function(event){
        event.preventDefault();
        data = $("#crossmatch-form").serializeArray()
        console.log(data)
        getCrossMatch(data[0].value,data[1].value,data[2].value)
        $("#loading").removeClass("invisible").addClass("visible")

        $("#crossmatch-form").find("button").html(buttonStatus)
        $("#crossmatch-form").prop("disabled",true)
        $("#accordion").html("")
    });





    function getCrossMatch(ra,dec,radio){
        var request = $.ajax({
            method:"POST",
            url:"http://127.0.0.1:8000/data/crossmatch/"+ra+"/"+dec+"/"+radio,
            data:{},
            dataType:"json",
        });
        request.done(function(result){
            if(result.hasOwnProperty("error")) {
                $("#error-msg").alert()
            }
            $("#loading").removeClass("visible").addClass("invisible")
            $("#crossmatch-form").find("button").html("Search")
            $("#crossmatch-form").prop("disabled",false)
            console.log(result)
            for( key  of Object.keys(result)){
                element=$(getSimplewrap(key))
                nitems=result[key].length
                element.find("button.collapsed").html(key+"["+nitems+"]")
                $("#accordion").append(element)
                jsonView.format(result[key], "#"+key+" .contenttree");
            }



         });

    }



});