import pathlib

from cldfbench import Dataset as BaseDataset, CLDFSpec


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "veselinovanegex"

    def cldf_specs(self):  # A dataset must declare all CLDF sets it creates.
        return CLDFSpec(module='StructureDataset', dir=self.cldf_dir)

    def cmd_download(self, args):
        self.raw_dir.xlsx2csv('NegEx_CLDF.xlsx')

    def cmd_makecldf(self, args):
        args.writer.cldf.add_component('LanguageTable')

        liso2gl = {l.iso: l for l in args.glottolog.api.languoids() if l.iso}
        language_errata = {r['NAM_LABEL']: r for r in self.etc_dir.read_csv('languages.csv', dicts=True)}
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
                args.writer.objects['ValueTable'].append(dict(
                    ID='{}-{}'.format(lid, pid),
                    Value=row[pid],
                    Language_ID=lid,
                    Parameter_ID=pid,
                    Comment=row['Comment'],
                ))

