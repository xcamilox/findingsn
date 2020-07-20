function createTable(dataset,columns,id){
    $.fn.dataTable.ext.errMode = 'none';
     var tableref=$(id).DataTable( {
        "lengthMenu": [ [-1, 10, 25, 50], [ "All",10, 25, 50] ],
        scrollY:        '60vh',
        scrollCollapse: true,
        paging:         true,
        scrollX: true,
        data: dataset,
        select:true,
        columns: columns,
        errMode: "throw",
        responsive: true
        // dom: 'Bfrtip',
        // buttons: [
        //     'copyHtml5',
        //     {
        //         text: 'TSV',
        //         extend: 'csvHtml5',
        //         fieldSeparator: '\t',
        //         extension: '.tsv'
        //     }
        // ]
    } );
     tableref.on( 'select', function ( e, dt, type, indexes ) {
        if ( type === 'row' ) {
            var data = tableref.rows( indexes ).data().toArray()[0];
            ztfid=data[0]

            document.dispatchEvent(new CustomEvent("itemSelected",{"detail":ztfid}))
        }
    } );
     return tableref
}