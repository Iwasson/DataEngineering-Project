import csv, json
from argparse import ArgumentParser
from geojson import Feature, FeatureCollection, Point
features = []

def convert(inpath, outpath):
    with open(inpath, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        data = csvfile.readlines()
        for line in data[1:len(data)-1]:
            line.strip()
            line = line.split('\n')[0]
            row = line.split(",")
            
            # skip the rows where speed is missing
            print(row)
            x = row[0]
            y = row[1]
            speed = row[2]
            if speed is None or speed == "":
                continue
        
            try:
                latitude, longitude = map(float, (y, x))
                features.append(
                    Feature(
                        geometry = Point((longitude,latitude)),
                        properties = {
                            'speed': (float(speed))
                        }
                    )
                )
            except ValueError:
                continue

    collection = FeatureCollection(features)
    with open(outpath, "w") as f:
        f.write('%s' % collection)

def parse_args():
    parser = ArgumentParser()
    parser.add_argument('-i', '--input', default='data.csv')
    parser.add_argument('-o', '--output', default='data.geojson')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    convert(args.input, args.output)
