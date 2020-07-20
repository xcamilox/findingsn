var aladin = A.aladin('#aladin-lite-div', {survey: "P/PanSTARRS/DR1/color/z/zg/g", fov:0.01});
var hips = A.catalogHiPS('https://axel.u-strasbg.fr/HiPSCatService/Simbad', {onClick: 'showTable', name: 'Simbad'});
aladin.addCatalog(hips);
references["aladin"]=aladin

function loadCatalogs(ra,dec){
    aladin.addCatalog(A.catalogFromNED(ra+","+dec, 10./3600., {onClick: 'showTable',shape: 'plus'}))
    aladin.addCatalog(A.catalogFromSimbad(ra+","+dec, 10./3600., {onClick: 'showTable'}))
}
