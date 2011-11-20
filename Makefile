include $(GOROOT)/src/Make.inc

TARG=dmrgo
GOFILES=\
	reporter.go\
	runners.go\
	proto.go\
	emitter.go

include $(GOROOT)/src/Make.pkg
