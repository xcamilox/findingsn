var newCandidates=[
    {
        '$match': {
            'state': 'new'
        }
    }, {
        '$project': {
            'maxmag_g': {
                '$arrayElemAt': [
                    '$lightpeak.stats.g.y', 1
                ]
            },
            'maxmag_r': {
                '$arrayElemAt': [
                    '$lightpeak.stats.r.y', 1
                ]
            },
            'ztfid': '$id',
            'nobsg': '$lightpeak.lightcurve.g.detections',
            'nobsr': '$lightpeak.lightcurve.r.detections',
            'photoz': {
                '$concat': [
                    {
                        '$toString': {
                            '$trunc': [
                                '$best_photo_z.photo_z', 4
                            ]
                        }
                    }, '+-', {
                        '$toString': {
                            '$trunc': [
                                '$best_photo_z.photo_zerr', 3
                            ]
                        }
                    }
                ]
            },
            'specz': {
                '$concat': [
                    {
                        '$toString': {
                            '$trunc': [
                                '$best_spec_z.spec_z', 4
                            ]
                        }
                    }, '+-', {
                        '$toString': {
                            '$trunc': [
                                '$best_spec_z.spec_zerr', 3
                            ]
                        }
                    }
                ]
            },
            'photo_archive': '$best_photo_z.photo_zarchive',
            'spec_archive': '$best_spec_z.spec_zarchive',
            'specz_d_arcsec': '$best_spec_z.spec_zsarcsec',
            'photoz_d_arcsec': '$best_photo_z.photo_zsarcsec',
            'phot_type': '$best_photo_z.photo_zobjtype',
            'spec_type': '$best_spec_z.spec_zobjtype',
            'g_state': '$lightpeak.status.g',
            'r_state': '$lightpeak.status.r',
            'spec_gABmag': '$best_specz_gabmag',
            'spec_rABmag': '$best_specz_rabmag',
            'phot_rABmag': '$best_photoz_rabmag',
            'phot_gABmag': '$best_photoz_gabmag',
            'broker': 1,
            'alerce_cls': '$alerce_early_class',
            'alerce_cls_late': '$alerce_late_class',
            'lasair_cls': '$lasair_clas',
            'tnsname': {
                '$arrayElemAt': [
                    '$crossmatch.tns.name', 0
                ]
            }
        }
    }
]