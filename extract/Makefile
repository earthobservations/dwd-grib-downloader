#cdo  sellonlatbox parameter:
# West+Oststeiermark
BBOX=14.5,16.8,46,47.5
#	not worth the effort: 58804 vs 47601
# -z jpeg
CDO_OPTS:=  --silent
INDIR=next/gribs
#icon-d2/2020120312/
OUTDIR=next-resized
#MERGE=next-resized/all.grib2

# test
#cdo  sellonlatbox parameter:
# West+Oststeiermark
BBOX=14.5,16.8,46,47.5
#	not worth the effort: 58804 vs 47601
# -z jpeg
CDO_OPTS:=  --silent
INDIR=test-downloader
#icon-d2/2020120312/
OUTDIR=test-downloader-resized
#MERGE=next-resized/all.grib2



STRUCTURE := $(shell find $(INDIR) -type d)
STRUCTURE_NO_INDIR := $(STRUCTURE:$(INDIR)=)
INGRIBS := $(shell find $(STRUCTURE) -name '*.grib2' -o -name '*.grib2.bz2')

# the list of files below OUTDIR, without OUTDIR prefix
INGRIBS_NO_INDIR := $(subst $(INDIR)/,,$(INGRIBS))

TARGETS := $(subst .bz2,,$(addprefix ${OUTDIR}/, $(INGRIBS:${INDIR}/%=%)))
TARGET_DIRS := $(addprefix ${OUTDIR}/, $(sort $(dir $(INGRIBS_NO_INDIR))))


STEPS := $(shell seq -f '%03.0f'  0 27)

default: target_directories $(TARGETS)
	$(info all up to date in $(OUTDIR))

# steps:
# ls next-resized/icon-d2/2020120312/|awk  '{split($0, a, "_"); print a[6]}'|sort|uniq
# step sequence: seq -f '%03.0f'  0 27

# merge: target_directories $(TARGETS)
# 	rm -f $(MERGE)
# 	$(foreach f,$(TARGETS),$(shell echo $(f); cdo -v cat $(f) $(MERGE)))

#	@SKIP_SAME_TIME=1 cdo merge $(TARGETS) $(MERGE)


# un-bzip2'd files
$(OUTDIR)/%.grib2: $(addprefix $(INDIR)/,%.grib2)
	@cdo  $(CDO_OPTS) sellonlatbox,$(BBOX) $<  $@

# bzip2'd files
$(OUTDIR)/%.grib2: $(addprefix $(INDIR)/,%.grib2.bz2)
			@$(eval TMP := $(shell mktemp  /tmp/tmpgrib2.XXXXXX))
			@bzip2 --keep --decompress --stdout $<  >$(TMP)
			@cdo  $(CDO_OPTS) sellonlatbox,$(BBOX) $(TMP) $@
			@rm -rf $(TMP)


steps:
		$(info $(STEPS))

structure:
	$(info  $(STRUCTURE))
	$(info  $(STRUCTURE_NO_INDIR))

clean:
	@rm -rf $(OUTDIR)

# ok below
target_directories:
	@mkdir -p $(TARGET_DIRS)
