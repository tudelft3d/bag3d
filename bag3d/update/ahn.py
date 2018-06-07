# -*- coding: utf-8 -*-

"""Update the AHN files and tile index"""


def update_json_id(json):
    """Update a GeoJSON tile index's ID field
    
    Overwrites or adds the "id" field as sequential integer
    
    Parameters
    ----------
    json: a parsed GeoJSON
    
    Returns
    -------
    The modified GeoJSON
    """
    for i,f in enumerate(json["features"]):
        json["features"][i]["id"] = i+1
        json["features"][i]["properties"]["id"] = i+1
    return json