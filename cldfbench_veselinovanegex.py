import re
import pathlib

from pybtex import errors
from pycldf.sources import Sources
from cldfbench import Dataset as BaseDataset, CLDFSpec


def citation(src):
    name, year = None, src.get('year')
    if 'author' in src:
        name = ' & '.join(
            s.split(',')[0]
            for s in src['author'].split(' and '))
    return '{} {}'.format(name, year)


def parse_citation(text):
    if not text:
        return None, None
    if ':' in text:
        ref, pages = text.split(':', maxsplit=1)
    else:
        ref, pages = text, None
    ref = ref.replace('(', '').replace(',', '') \
        .replace('MIchael', 'Michael') \
        .replace('MIestam', 'Miestam') \
        .replace('Brandup', 'Brandrup')
    ref = re.sub(r'\s+', ' ', ref)
    ref = ref.replace(' and ', ' & ')
    if 'p.c.' not in ref.replace(' ', ''):
        return ref, pages.strip() if pages else pages
    return None, None


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "veselinovanegex"

    def cldf_specs(self):  # A dataset must declare all CLDF sets it creates.
        return CLDFSpec(module='StructureDataset', dir=self.cldf_dir)

    def cmd_download(self, args):
        self.raw_dir.xlsx2csv('NegEx_CLDF.xlsx')

    def cmd_makecldf(self, args):
        errors.strict = False
        sources = {}
        for src in Sources.from_file(self.raw_dir / 'NegEx_bib.txt'):
            sources[citation(src)] = src
        args.writer.cldf.add_sources(*list(sources.values()))
        args.writer.cldf.add_sources("""
@article{veselinova2013,
    author = {Ljuba Veselinova},
    year = {2013},
    journal = {Rivista di Linguistica},
    volume = {25},
    issue = {1},
    pages = {107-145}
}
""")
        args.writer.cldf.add_component('LanguageTable')
        args.writer.cldf.add_component(
            'ParameterTable',
            {
                'name': 'Grammacodes',
                'separator': ';',
            })
        args.writer.cldf.add_component('CodeTable', 'Map_Icon')

        liso2gl = {
            languoid.iso: languoid
            for languoid in args.glottolog.api.languoids() if languoid.iso}
        language_errata = {
            r['NAM_LABEL']: r
            for r in self.etc_dir.read_csv('languages.csv', dicts=True)}
        parameters = list(
            self.etc_dir.read_csv('parameters.csv', dicts=True))
        codes = {
            (row['Parameter_ID'], row['Original_Name']): row
            for row in self.etc_dir.read_csv('codes.csv', dicts=True)}
        for parameter in parameters:
            if (grammacodes := parameter.get('Grammacodes')):
                parameter['Grammacodes'] = [
                    id_.strip()
                    for id_ in grammacodes.split(',')]
        args.writer.objects['ParameterTable'] = parameters
        args.writer.objects['CodeTable'] = codes.values()
        for row in self.raw_dir.read_csv('NegEx_CLDF.NegExCLLD.csv', dicts=True):
            if row['NAM_LABEL'] in language_errata:
                row.update(language_errata[row['NAM_LABEL']])
            lid = row['ID_ISO_A3']
            glang = liso2gl.get(lid)
            args.writer.objects['LanguageTable'].append(dict(
                ID=lid,
                Name=row['NAM_LABEL'],
                ISO639P3code=lid,
                Glottocode=glang.id if glang else None,
                Latitude=glang.latitude if glang else None,
                Longitude=glang.longitude if glang else None,
            ))
            for pid in ['SN', 'NegEx_Form', 'NegExType']:
                refs = []
                if pid == 'SN':
                    ref, pages = parse_citation(row['Source_SN'])
                    if ref in sources:
                        key = sources[ref].id
                        if pages:
                            key += '[{}]'.format(pages)
                        refs.append(key)
                elif pid == 'NegEx_Form':
                    ref, pages = parse_citation(row['Source_NegEx'])
                    if ref in sources:
                        key = sources[ref].id
                        if pages:
                            key += '[{}]'.format(pages)
                        refs.append(key)
                else:
                    refs = []
                original_value = row[pid]
                code = codes.get((pid, original_value))
                code_id = code['ID'] if code else ''
                value = code['Name'] if code else original_value
                args.writer.objects['ValueTable'].append(dict(
                    ID='{}-{}'.format(lid, pid),
                    Code_ID=code_id,
                    Value=value,
                    Language_ID=lid,
                    Parameter_ID=pid,
                    Source=refs,
                ))
